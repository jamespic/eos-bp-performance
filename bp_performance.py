#!/usr/bin/env python
import argparse
import datetime
import itertools
from jinja2 import Template
import json
import pygal
import time
import threading
import traceback
from collections import defaultdict, deque, Counter
from ciso8601 import parse_datetime
from concurrent.futures import ThreadPoolExecutor
from cheroot.wsgi import Server, PathInfoDispatcher
from urllib.request import urlopen
from werkzeug.wsgi import pop_path_info


class BPPerformance:
    def __init__(self, classifiers, endpoint="http://localhost:8888", max_count=10, max_age=86400):
        self._endpoint = endpoint
        self._classifiers = classifiers
        self._max_count = max_count
        self._max_age = max_age
        self._stopped = True
        self._stats = defaultdict(lambda: defaultdict(deque))
        self._last_timestamp = datetime.datetime(1970, 1, 1)
        self.unknown = Counter()

    def watch(self):
        self._stopped = False
        self.last_block_num = self._last_irreversible_block_number()
        with ThreadPoolExecutor(4) as executor:
            while not self._stopped:
                time.sleep(1.0)
                try:
                    block_num = self._last_irreversible_block_number()
                    for i in range(self.last_block_num + 1, block_num + 1):
                        executor.submit(self._handle_block, i)
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

    def _handle_block(self, block_num):
        block = self._get_block(block_num)
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

def chart_renderer(bp_perf):
    def render_chart(environ, start_response):
        stats = bp_perf.stats
        chart_name = pop_path_info(environ)
        if chart_name not in stats:
            start_response('404 Not Found', [('content-type', 'text/plain; charset=ascii')])
            return [b"Chart not found"]
        else:
            data = stats[chart_name]
            chart = pygal.Box(box_mode='tukey')
            chart.title = chart_name
            for bp, data in data.items():
                chart.add(bp, data)
            start_response('200 OK', [('content-type', 'image/svg+xml')])
            return [chart.render()]
    return render_chart

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
              <div id="accordion">
                {% for chart in charts.keys() %}
                  <div class="card">
                    <div class="card-header" id="heading{{ chart.replace(' ', '') }}">
                      <h5 class="mb-0">
                        <button class="btn btn-link"
                            data-toggle="collapse"
                            data-target="#{{ chart.replace(' ', '') }}"
                            aria-expanded="false" aria-controls="{{ chart.replace(' ', '') }}">
                          {{ chart }}
                        </button>
                      </h5>
                    </div>

                    <div id="{{ chart.replace(' ', '') }}"
                        class="collapse"
                        aria-labelledby="heading{{ chart.replace(' ', '') }}"
                        data-parent="#accordion">
                      <div class="card-body">
                        <object data="/chart/{{ chart | urlencode | escape }}"></object>
                      </div>
                    </div>
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
    ('eosknightsio', 'rebirth'): lambda x: 'EOS Knights Rebirth',
    ('eosio', 'delegatebw'): lambda x: 'Delegate resources',
    ('eosio', 'undelegatebw'): lambda x: 'Undelegate resources',
    ('eosio', 'voteproducer'): lambda x: 'Block producer vote',
    ('eosio', 'buyram'): lambda x: 'Buy RAM',
    ('eosio', 'sellram'): lambda x: 'Sell RAM'
}


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

    app = PathInfoDispatcher({
        '/': index(bp_perf),
        '/chart': chart_renderer(bp_perf)
    })

    httpd = Server((args.host, args.port), app)

    if args.certificate:
        from cheroot.ssl.builtin import BuiltinSSLAdapter
        httpd.ssl_adapter = BuiltinSSLAdapter(args.certificate, args.key)

    try:
        print(f"Serving on {args.host}:{args.port}")
        httpd.safe_start()
    finally:
        bp_perf.stop()
