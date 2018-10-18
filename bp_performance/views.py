import csv
import io
import pygal
import re
import wrapt

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
        chart = pygal.DateTimeLine(width=WIDTH, height=HEIGHT)
        chart.title = title or filename
        for series_name, series in data.items():
            chart.add(series_name, list(series.items()))
        rendered = chart.render()
        start_response('200 OK', [('Content-Type', 'image/svg+xml')])
        return [rendered]
