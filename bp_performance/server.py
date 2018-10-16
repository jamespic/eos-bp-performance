from .models import Database
from .controllers import yaml_time_range
from cheroot.wsgi import Server, PathInfoDispatcher

if __name__ == '__main__':
    db = Database('/tmp/tempdb', 'https://localhost:8888')
    app = PathInfoDispatcher({
        '/raw_data.yaml': yaml_time_range(db)
    })

    httpd = Server(('0.0.0.0', 8094), app)
    httpd.safe_start()
