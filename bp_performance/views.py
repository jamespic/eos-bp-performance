import wrapt
import pygal

WIDTH, HEIGHT = 1200, 600

def time_graph(data, environ, start_response, title=None, filename=''):
    if filename.endswith('.csv'):
        pass  # FIXME
    else:
        chart = pygal.DateTimeLine(width=WIDTH, height=HEIGHT)
        chart.title = title or filename
        for series_name, series in data.items():
            chart.add(series_name, series)
        rendered = chart.render()
        start_response('200 OK', [('Content-Type', 'application/svg+xml')])
        return [rendered]
