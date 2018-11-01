import csv
import io
import pygal
import re
import wrapt

from urllib.parse import parse_qs
from pygal.util import cached_property

WIDTH, HEIGHT = 1200, 600

def time_graph(data, environ, start_response, title=None, filename=''):
    if filename.endswith('.csv'):
        filename = re.sub(r'[^\.\w]+', '_', filename)
        buf = io.StringIO()
        timestamps = sorted(set(key for series in data.values() for key in series.keys()))
        writer = csv.DictWriter(buf, ['Timestamp'] + list(data.keys()))
        writer.writeheader()
        for timestamp in timestamps:
            row = dict(Timestamp=timestamp.isoformat(), **{
                series_name: str(series_data.get(timestamp, ''))
                for series_name, series_data in data.items()
                if timestamp in series_data
            })
            writer.writerow(row)
        start_response('200 OK', [
            ('Content-Type', 'text/csv; charset=utf-8'),
            ('Content-Disposition', f'attachment; filename="{filename}"')
        ])
        return [buf.getvalue().encode('utf-8')]
    else:
        chart = pygal.DateTimeLine(**width_and_height(environ))
        chart.title = title or filename
        for series_name, series in data.items():
            chart.add(series_name, list(series.items()))
        rendered = chart.render()
        start_response('200 OK', [('Content-Type', 'image/svg+xml')])
        return [rendered]

class _StatsBoxPlot(pygal.Box):
    def _box_points(self, values, mode=None):
        return [values[0].quantile(x) for x in [0.0, 0.05, 0.25, 0.5, 0.75, 0.95, 1.0]], []

    def _value_format(self, values, serie):
        return 'Min: {0}\n5th Percentile : {1}\n25th Percentile : {2}\nMedian : {3}\n75th Percentile {4}\n95th Percentile: {5}\nMax: {6}'.format(*[self._y_format(x) for x in serie.points])

    @cached_property
    def _min(self):
        """Getter for the minimum series value"""
        return (
            self.range[0] if (self.range and self.range[0] is not None) else
            (min(x.quantile(0.05) for x in self._values) if self._values else None)
        )

    @cached_property
    def _max(self):
        """Getter for the maximum series value"""
        return (
            self.range[1] if (self.range and self.range[1] is not None) else
            (max(x.quantile(0.95) for x in self._values) if self._values else None)
        )

def stats_box_plot(data, environ, start_response, title=None, filename=''):
    if filename.endswith('.csv'):
        raise NotImplementedError()
    else:
        chart = _StatsBoxPlot(**width_and_height(environ))
        chart.title = title or filename
        for series_name, series in data.items():
            chart.add(series_name, series)
        rendered = chart.render()
        start_response('200 OK', [('Content-Type', 'image/svg+xml')])
        return [rendered]

def width_and_height(environ):
    qs = environ.get('QUERY_STRING', '')
    query = parse_qs(qs)
    width = int(query['width'][-1]) if 'width' in query else WIDTH
    height = int(query['height'][-1]) if 'height' in query else HEIGHT
    return {'width': width, 'height': height}
