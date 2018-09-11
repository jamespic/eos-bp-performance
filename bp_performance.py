#!/usr/bin/env python
import argparse
import csv
import datetime
import io
import itertools
import json
import numpy
import pygal
import time
import threading
import traceback
import sys
from collections import defaultdict, deque, Counter, namedtuple
from ciso8601 import parse_datetime
from concurrent.futures import ThreadPoolExecutor
from cheroot.wsgi import Server, PathInfoDispatcher
from jinja2 import Template
from urllib.request import urlopen
from werkzeug.wsgi import pop_path_info
from werkzeug.wrappers import Request, Response

_BlockSummary = namedtuple(
    '_BlockSummary',
    ['timestamp', 'producer', 'slot_position', 'produced', 'action_counts']
)

class BPPerformance:
    def __init__(self, classifiers, endpoint="http://localhost:8888", max_count=300, max_age=86400):
        self._endpoint = endpoint
        self._classifiers = classifiers
        self._max_count = max_count
        self._max_age = max_age
        self._stopped = True
        self._stats = defaultdict(lambda: defaultdict(deque))
        self._block_summaries = deque()
        self._last_timestamp = datetime.datetime(1970, 1, 1)
        self._schedules = {}
        self.unknown = Counter()

    def watch(self):
        self._stopped = False
        self.last_block_num = self._last_irreversible_block_number() - 7200  # Prepopulate with last hour
        self._find_producer_schedules()
        with ThreadPoolExecutor(4) as executor:
            while not self._stopped:
                time.sleep(1.0)
                try:
                    block_num = self._last_irreversible_block_number()
                    if block_num != self.last_block_num:
                        print(f"Fetching data for blocks {self.last_block_num + 1} to {block_num}", file=sys.stderr)
                        for block in executor.map(
                                self._get_block,
                                range(self.last_block_num + 1, block_num + 1)):
                            self._handle_block(block)
                        self.last_block_num = block_num
                except Exception:  # Can we do better than this?
                    traceback.print_exc()
                    time.sleep(60)

    def stop(self):
        self._stopped = True

    @property
    def stats(self):
        return {
            category: {
                bp: [cpu for timestamp, cpu in timings]
                for bp, timings in sorted(bps.items())
                if self._trim_stats(timings, self._last_timestamp)
            }
            for category, bps in self._stats.items()
        }

    @property
    def missed_blocks(self):
        result = defaultdict(lambda: [[] for _ in range(12)])
        for block in self._block_summaries:
            result[block.producer][block.slot_position].append(block.produced)
        return {
            producer: [
                sum(slot_data) * 100.0 / len(slot_data) if slot_data else 100.0
                for slot_data in slots
            ] for producer, slots in sorted(result.items())
        }

    def _handle_block_transactions(self, block):
        timestamp = parse_datetime(block['timestamp'])
        producer = block['producer']
        for tx in block['transactions']:
            cpu = tx['cpu_usage_us']
            if isinstance(tx['trx'], dict):
                actions = tx['trx']['transaction']['actions']
                if len(actions) == 1:
                    action = actions[0]
                    classifier = self._classifiers.get(
                        (action['account'], action['name'])
                    )
                    if classifier:
                        category = classifier(action)
                        if category:
                            self._store_value(producer, category, timestamp, cpu)
                    else:
                        self.unknown[f"{action['account']} {action['name']}"] += 1

    def _handle_block_summaries(self, block):
        timestamp = parse_datetime(block['timestamp'])
        if self._block_summaries:
            # Fill in gaps in producer schedule
            last_block = self._block_summaries[-1]
            last_timestamp = last_block.timestamp
            slots_missed = int((timestamp - last_timestamp).total_seconds() * 2) - 1
            for i in range(slots_missed):
                schedule = self._schedules.get(block['schedule_version'])
                if schedule:
                    missed_timestamp = last_timestamp + (i + 1) * datetime.timedelta(seconds=0.5)
                    producer, slot_position = _block_producer_for_timestamp(missed_timestamp, schedule)
                    missed_block_summary = _BlockSummary(missed_timestamp, producer, slot_position, False, Counter())
                    self._block_summaries.append(missed_block_summary)
        if block['new_producers']:
            self._load_schedule(block['new_producers'])
        schedule = self._schedules.get(block['schedule_version'])
        if schedule:
            expected_producer, slot_position = _block_producer_for_timestamp(timestamp, schedule)
            assert expected_producer == block['producer']
            action_counts = Counter()
            for tx in block['transactions']:
                if isinstance(tx['trx'], dict):
                    actions = tx['trx']['transaction']['actions']
                    for action in actions:
                        action_counts[(action['account'], action['name'])] += 1
            block_summary = _BlockSummary(timestamp, block['producer'], slot_position, True, action_counts)
            self._block_summaries.append(block_summary)
        while len(self._block_summaries) > self._max_age * 2:
            self._block_summaries.popleft()


    def _find_producer_schedules(self):
        info = json.load(urlopen(f"{self._endpoint}/v1/chain/get_info"))
        head_block_num = info['head_block_num']
        header_block_state = json.load(
            urlopen(
                f"{self._endpoint}/v1/chain/get_block_header_state",
                json.dumps({"block_num_or_id":head_block_num}).encode('utf-8')
            )
        )
        self._load_schedule(header_block_state['active_schedule'])
        pending_schedule_version = header_block_state['pending_schedule']['version']
        if pending_schedule_version not in self._schedules:
            self._load_schedule(header_block_state['pending_schedule'])

    def _load_schedule(self, schedule):
        version = schedule['version']
        self._schedules[version] = [producer['producer_name'] for producer in schedule['producers']]

    def _handle_block(self, block):
        self._handle_block_transactions(block)
        self._handle_block_summaries(block)
        timestamp = parse_datetime(block['timestamp'])
        self._last_timestamp = timestamp

    def _store_value(self, producer, category, timestamp, time):
        queue = self._stats[category][producer]
        queue.append((timestamp, time))
        self._trim_stats(queue, timestamp)

    def _trim_stats(self, queue, timestamp):
        while len(queue) > self._max_count:
            queue.popleft()
        min_time = timestamp - datetime.timedelta(seconds=self._max_age)
        while queue:
            head = queue[0]
            if head[0] < min_time:
                queue.remove(head)
            else:
                break
        return queue

    def _last_irreversible_block_number(self):
        info = json.load(urlopen(f"{self._endpoint}/v1/chain/get_info"))
        return info['last_irreversible_block_num']

    def _get_block(self, block):
        return json.load(
            urlopen(
                f"{self._endpoint}/v1/chain/get_block",
                json.dumps({
                    "block_num_or_id": str(block)
                }).encode('utf-8')
            )
        )

def _timestamp_to_slot(timestamp):
    epoch_time = timestamp - datetime.datetime.fromtimestamp(946684800)
    return int(epoch_time.total_seconds() * 2)

def _block_producer_for_timestamp(timestamp, schedule):
    slot = _timestamp_to_slot(timestamp)
    return schedule[(slot % (len(schedule) * 12)) // 12], slot % 12

def chart_renderer(bp_perf):
    def render_chart(environ, start_response):
        stats = bp_perf.stats
        chart_name = pop_path_info(environ)
        if chart_name not in stats:
            start_response('404 Not Found', [('content-type', 'text/plain; charset=ascii')])
            return [b"Chart not found"]
        else:
            data = stats[chart_name]
            chart = pygal.Box(box_mode='tukey', width=1000, height=600)
            chart.title = chart_name
            for bp, data in data.items():
                chart.add(bp, data)
            start_response('200 OK', [('content-type', 'image/svg+xml')])
            return [chart.render()]
    return render_chart

def csv_dump(bp_perf):
    def render_csv(environ, start_response):
        output_file = io.StringIO()
        writer = csv.DictWriter(
            output_file, [
                "Transaction Type",
                "Block Producer",
                "Minimum",
                "First Quartile",
                "Median",
                "Mean",
                "Third Quartile",
                "99th Percentile",
                "Maximum",
                "Count"
            ]
        )
        writer.writeheader()
        for tx_type, tx_data in bp_perf.stats.items():
            for bp, timing_data in tx_data.items():
                writer.writerow({
                    "Transaction Type": tx_type,
                    "Block Producer": bp,
                    "Minimum": min(timing_data),
                    "First Quartile": numpy.percentile(timing_data, 25),
                    "Median": numpy.percentile(timing_data, 50),
                    "Mean": sum(timing_data) / len(timing_data),
                    "Third Quartile": numpy.percentile(timing_data, 75),
                    "99th Percentile": numpy.percentile(timing_data, 99),
                    "Maximum": max(timing_data),
                    "Count": len(timing_data)
                })
        result = output_file.getvalue().encode('utf-8')
        start_response('200 OK', [
            ('Content-Type', 'text/csv; charset=utf-8'),
            ('Content-Disposition', 'attachment; filename="bp-perf.csv"')
        ])
        return [result]
    return render_csv

def missed_slots(bp_perf):
    def render_slots(environ, start_response):
        data = bp_perf.missed_blocks
        if environ.get('HTTP_ACCEPT') == 'application/json':
            start_response('200 OK', [('Content-Type', 'application/json')])
            return [json.dumps(data).encode('utf-8')]
        else:
            chart = pygal.Bar(width=1000, height=600)
            chart.title = 'Missed Slots'
            chart.x_labels = data.keys()
            for i in range(12):
                chart.add(f"Slot {i}", [slots[i] for slots in data.values()])
            start_response('200 OK', [('content-type', 'image/svg+xml')])
            return [chart.render()]
    return render_slots

def index(bp_perf):
    def render_index(environ, start_response):
        template = Template("""<!DOCTYPE html>
        <html>
          <head>
            <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
            <link rel="stylesheet"
              href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css"
              integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm"
              crossorigin="anonymous">
            <title>Block Producer Performance</title>
          </head>
          <body>
            <nav class="navbar navbar-dark" style="background-color: #a301b5;">
              <div class="navbar-brand">Block Producer Performance</div>
            </nav>
            <div class="container">
              <ul class="nav nav-pills" style="padding-top: 1rem;">
                <li class="nav-item">
                  <a class="nav-link active"
                      id="about-tab"
                      data-toggle="tab"
                      href="#about"
                      role="tab"
                      aria-controls="about"
                      aria-selected="true">
                    About
                  </a>
                </li>
                <li class="nav-item">
                  <a class="nav-link"
                      id="missed-slots-tab"
                      data-toggle="tab"
                      href="#missed-slots"
                      role="tab"
                      aria-controls="missed-slots"
                      aria-selected="true">
                    Missed Slots
                  </a>
                </li>
                {% for chart in charts.keys() %}
                  <li class="nav-item">
                    <a class="nav-link"
                        id="{{ chart.replace(' ', '') }}-tab"
                        data-toggle="tab"
                        href="#{{ chart.replace(' ', '') }}"
                        role="tab"
                        aria-controls="{{ chart.replace(' ', '') }}"
                        aria-selected="true">
                      {{ chart }}
                    </a>
                  </li>
                {% endfor %}
              </ul>
              <div class="tab-content" style="padding-top: 1rem;">
                <div class="tab-pane active"
                    id="about"
                    role="tabpanel"
                    aria-labelledby="about-tab">
                  <p>
                    CPU billing in EOS is a bit different to other
                    smart-contract based chains. CPU usage isn't objectively
                    calculated, but measured - block producers run your
                    transaction, time how long it took, and bill you
                    accordingly.
                  </p>
                  <p>
                    This makes it vitally important that you vote
                    for good block producers. If a block producer uses cheap
                    hardware, then they'll end up billing too much CPU, or
                    billing CPU unfairly, which limits chain scalability. Even
                    worse, a malicious block producer could overcharge an
                    account that they don't like.
                  </p>
                  <p>
                    Luckily, it's possible to keep an eye on how much block
                    producers are billing.
                  </p>
                  <p>
                    This site graphs the time block producers bill for a
                    selection of common transaction types, over the last day
                    (or the last 300 transactions of that type for the most
                    common transaction types). Lower numbers are better, and
                    more consistent numbers are better (on the box plots, this
                    means bigger boxes are bad, and outliers, points way
                    outside the boxes, are bad).
                  </p>
                  <p>
                    If you've found this useful, consider donating to
                    <tt>gmyteojxgmge</tt>. Right now, this site runs on my
                    crappy home server, and with some donations, I could rent
                    some less crappy hardware. The source code is at
                    <a href="https://github.com/jamespic/eos-bp-performance">
                    https://github.com/jamespic/eos-bp-performance</a>.
                  </p>
                  <p>
                    <a href="/csv">
                      Click here to download a CSV dump of the summary data.
                    </a>
                  </p>
                </div>
                <div class="tab-pane"
                    id="missed-slots"
                    role="tabpanel"
                    aria-labelledby="missed-slots-tab">
                  <object data="/missed_slots"></object>
                </div>
                {% for chart in charts.keys() %}
                  <div class="tab-pane"
                      id="{{ chart.replace(' ', '') }}"
                      role="tabpanel"
                      aria-labelledby="{{ chart.replace(' ', '') }}-tab">
                    <object data="/chart/{{ chart | urlencode | escape }}"></object>
                  </div>
                {% endfor %}
              </div>
            </div>
            <script src="https://code.jquery.com/jquery-3.2.1.slim.min.js"
              integrity="sha384-KJ3o2DKtIkvYIK3UENzmM7KCkRr/rE9/Qpg6aAZGJwFDMVNA/GpGFF93hXpG5KkN"
              crossorigin="anonymous"></script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js"
              integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q"
              crossorigin="anonymous"></script>
            <script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js"
              integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl"
              crossorigin="anonymous"></script>
          </body>
        </html>
        """)
        rendered = template.render(charts=bp_perf.stats)
        start_response('200 OK', [('content-type', 'text/html; charset=utf-8')])
        return [rendered.encode('utf-8')]
    return render_index


classifiers = {
    ('eosio.token', 'transfer'): lambda x: 'Simple Transfer',
    ('blocktwitter', 'tweet'): lambda x: 'WE LOVE BM' if x['data']['message'] == 'WE LOVE BM' else None,
    ('eosbetdice11', 'resolvebet'): lambda x: 'EOS Bet',
    ('eosknightsio', 'rebirth2'): lambda x: 'EOS Knights Rebirth',
    ('prochaintech', 'click'): lambda x: 'Prochain Click',
    ('eosio', 'delegatebw'): lambda x: 'Delegate resources',
    ('eosio', 'undelegatebw'): lambda x: 'Undelegate resources',
    ('eosio', 'voteproducer'): lambda x: 'Block producer vote',
    ('eosio', 'buyram'): lambda x: 'Buy RAM',
    ('eosio', 'sellram'): lambda x: 'Sell RAM'
}


def cache_middleware(expiry_seconds):
    expiry = datetime.timedelta(seconds=expiry_seconds)
    cache = {}
    def wrapper(f):
        def wrapped(environ, start_response):
            req = Request(environ)
            path = req.path
            cached = cache.get(path)
            if cached and not (
                    req.cache_control.no_store or req.cache_control.no_cache):
                updated_time, content, status, headers = cached
                if updated_time + expiry > datetime.datetime.now():
                    # Cached version still valid
                    browser_cache_valid = False
                    etag = str(hash(content))
                    if req.if_none_match.contains(etag):
                        browser_cache_valid = True
                    if req.if_modified_since:
                        if updated_time < req.if_modified_since:
                            browser_cache_valid = True
                    if browser_cache_valid:
                        start_response('304 Not Modified', [])
                        return []
                    else:
                        response = Response(content, status, headers)
                        response.add_etag(etag)
                        response.date = datetime.datetime.now()
                        return response(environ, start_response)
            # Cached version not valid
            sent_status = None
            sent_headers = []
            def inner_start_response(status, headers, exc=None):
                nonlocal sent_status, sent_headers
                if exc:
                    start_response(status, headers, exc)
                else:
                    sent_status = status
                    sent_headers = list(headers)
            response_iter = f(environ, inner_start_response)
            if sent_status:
                # Success! Cache and send response
                content = tuple(response_iter)
                cache[path] = (datetime.datetime.now(), content, sent_status, sent_headers)
                response = Response(content, sent_status, sent_headers)
                response.set_etag(str(hash(content)))
                response.date = datetime.datetime.now()
                return response(environ, start_response)
            else:
                # Don't cache the exception handler, just propagate
                return response_iter
        return wrapped
    return wrapper

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Run a web server with stats about EOS block producer performance")
    parser.add_argument('--nodeos-url', nargs='?', default="http://localhost:8888")
    parser.add_argument('--host', nargs='?', default='0.0.0.0')
    parser.add_argument('--port', nargs='?', default=8953, type=int)
    parser.add_argument('--certificate', nargs='?', help='TLS cert location')
    parser.add_argument('--key', nargs='?', help='TLS private key location')
    args = parser.parse_args()
    bp_perf = BPPerformance(classifiers, endpoint=args.nodeos_url)
    thread = threading.Thread(target=bp_perf.watch)
    thread.start()

    app = cache_middleware(60)(
        PathInfoDispatcher({
            '/': index(bp_perf),
            '/chart': chart_renderer(bp_perf),
            '/csv': csv_dump(bp_perf),
            '/missed_slots': missed_slots(bp_perf)
        })
    )

    httpd = Server((args.host, args.port), app)

    if args.certificate:
        from cheroot.ssl.builtin import BuiltinSSLAdapter
        httpd.ssl_adapter = BuiltinSSLAdapter(args.certificate, args.key)

    try:
        print(f"Serving on {args.host}:{args.port}")
        httpd.safe_start()
    finally:
        bp_perf.stop()
