import datetime
import yaml

from ciso8601 import parse_datetime
from urllib.parse import parse_qs
from werkzeug.wsgi import pop_path_info

from .views import time_graph, stats_box_plot, bar_chart


def transactions_over_time(db):
    def render_transactions_over_time(environ, start_response):
        query_string = environ.get('QUERY_STRING', '')
        filename = pop_path_info(environ)
        method = filename.rsplit('.', 1)[0]
        raw_data = _data_from_range_query_string(db, query_string)
        query = parse_qs(query_string)
        quantile = float(query['percentile'][-1]) / 100 if 'percentile' in query else 0.90
        producers = sorted({
            producer_name
            for block in raw_data.values()
            for producer_name, bp_data in block.producers.items()
            if method in bp_data.tx_data
        })
        data = {
            producer_name: {
                timestamp:
                    block.producers[producer_name].tx_data[method].quantile(quantile)
                for timestamp, block in raw_data.items()
                if producer_name in block.producers
                and method in block.producers[producer_name].tx_data
            } for producer_name in producers
        }
        return time_graph(data, environ, start_response, method, filename)
    return render_transactions_over_time


def transaction_box(db):
    def render_transactions_box(environ, start_response):
        query_string = environ.get('QUERY_STRING', '')
        filename = pop_path_info(environ)
        method = filename.rsplit('.', 1)[0]
        raw_data = _data_from_single_query_string(db, query_string)
        query = parse_qs(query_string)
        quantile = float(query['percentile'][-1]) / 100 if 'percentile' in query else 0.90
        data = {
            producer_name: producer_data.tx_data[method]
            for producer_name, producer_data in sorted(raw_data.producers.items())
        }
        return stats_box_plot(data, environ, start_response, method, filename)
    return render_transactions_box


def missed_slots_over_time(db):
    def render_missed_slots_over_time(environ, start_response):
        query_string = environ.get('QUERY_STRING', '')
        filename = pop_path_info(environ)
        raw_data = _data_from_range_query_string(db, query_string)
        producers = sorted({
            producer_name
            for block in raw_data.values()
            for producer_name in block.producers.keys()
        })
        data = {
            producer_name: {
                timestamp:
                    total_blocks_missed_percent(block.producers[producer_name])
                for timestamp, block in raw_data.items()
                if producer_name in block.producers
            } for producer_name in producers
        }
        return time_graph(data, environ, start_response, 'Missed Slots', filename)
    return render_missed_slots_over_time


def missed_slots(db):
    def render_missed_slots(environ, start_response):
        query_string = environ.get('QUERY_STRING', '')
        filename = pop_path_info(environ)
        raw_data = _data_from_single_query_string(db, query_string)
        x_labels = sorted(raw_data.producers.keys())
        data = {
            f'Slot {i + 1}': [
                blocks_missed_percent(data, i)
                for producer, data in sorted(raw_data.producers.items())
            ] for i in range(12)
        }
        return bar_chart(x_labels, data, environ, start_response, 'Missed Slots', filename)
    return render_missed_slots


def total_blocks_missed_percent(producer):
    return 100 * (producer.slots_passed_total - producer.blocks_produced_total) / producer.slots_passed_total

def blocks_missed_percent(producer, slot):
    return 100 * (producer.slots_passed[slot] - producer.blocks_produced[slot]) / producer.slots_passed[slot]

def yaml_time_range(db):
    def render_yaml_time_range(environ, start_response):
        data = _data_from_range_query_string(db, environ.get('QUERY_STRING', ''))
        result = yaml.dump(data).encode('utf-8')
        start_response('200 OK', [('Content-Type', 'application/yaml')])
        return [result]
    return render_yaml_time_range

def yaml_single(db):
    def render_yaml_time_range(environ, start_response):
        data = _data_from_single_query_string(db, environ.get('QUERY_STRING', ''))
        result = yaml.dump(data).encode('utf-8')
        start_response('200 OK', [('Content-Type', 'application/yaml')])
        return [result]
    return render_yaml_time_range

def _data_from_range_query_string(db, qs):
    query = parse_qs(qs)
    start = parse_datetime(query['from'][-1]) if 'from' in query else None
    end = parse_datetime(query['to'][-1]) if 'to' in query else None
    step = datetime.timedelta(seconds=float(query['step'][-1]) if 'step' in query else 1260)
    return db.fetch_by_time_range(start, end, step)

def _data_from_single_query_string(db, qs):
    query = parse_qs(qs)
    start = parse_datetime(query['from'][-1]) if 'from' in query else None
    end = parse_datetime(query['to'][-1]) if 'to' in query else None
    return db.fetch_single(start, end)
