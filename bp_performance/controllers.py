import datetime
import yaml

from ciso8601 import parse_datetime
from urllib.parse import parse_qs

from .views import time_graph


def transactions_over_time(db):
    def render_transactions_over_time(environ, start_response):
        pass  # FIXME
    return render_transactions_over_time


def yaml_time_range(db):
    def render_yaml_time_range(environ, start_response):
        query = parse_qs(environ.get('QUERY_STRING'))
        start = parse_datetime(query['from'][-1]) if 'from' in query else None
        end = parse_datetime(query['to'][-1]) if 'to' in query else None
        step = datetime.timedelta(seconds=float(query['step'][-1])) if 'step' in query else None
        data = db.fetch_by_time_range(start, end, step)
        result = yaml.dump(data).encode('utf-8')
        start_response('200 OK', [('Content-Type', 'application/yaml')])
        return [result]
    return render_yaml_time_range
