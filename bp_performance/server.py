from .controllers import yaml_time_range, yaml_single, transactions_over_time
from .middleware import cache_middleware
from .models import Database
from cheroot.wsgi import Server, PathInfoDispatcher

if __name__ == '__main__':
    db = Database('/tmp/tempdb', 'https://localhost:8888')
    app = cache_middleware(60)(PathInfoDispatcher({
        '/range.yaml': yaml_time_range(db),
        '/single.yaml': yaml_single(db),
        '/transactions-over-time': transactions_over_time(db)
    }))


    httpd = Server(('0.0.0.0', 8094), app)
    httpd.safe_start()
