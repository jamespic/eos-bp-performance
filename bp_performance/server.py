import argparse
import threading
from pathlib import Path
from cheroot.wsgi import Server, PathInfoDispatcher
from .controllers import yaml_time_range, yaml_single, transactions_over_time, transaction_box, missed_slots_over_time
from .middleware import cache_middleware
from .models import Database

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Run a web server with stats about EOS block producer performance")
    parser.add_argument('--nodeos-url', nargs='?', default="http://localhost:8888")
    parser.add_argument('--database-path', nargs='?', default=str(Path.home() / 'bp-perf-db'))
    parser.add_argument('--host', nargs='?', default='0.0.0.0')
    parser.add_argument('--port', nargs='?', default=8953, type=int)
    parser.add_argument('--certificate', nargs='?', help='TLS cert location')
    parser.add_argument('--key', nargs='?', help='TLS private key location')
    parser.add_argument('--no-sync', action='store_false', dest='sync')
    args = parser.parse_args()

    db = Database(args.database_path, args.nodeos_url)
    app = cache_middleware(1260)(PathInfoDispatcher({
        '/range.yaml': yaml_time_range(db),
        '/single.yaml': yaml_single(db),
        '/transactions-over-time': transactions_over_time(db),
        '/transactions-box': transaction_box(db),
        '/missed-slots-over-time': missed_slots_over_time(db)
    }))


    httpd = Server((args.host, args.port), app)

    if args.certificate:
        from cheroot.ssl.builtin import BuiltinSSLAdapter
        httpd.ssl_adapter = BuiltinSSLAdapter(args.certificate, args.key)

    if args.sync:
        thread = threading.Thread(target=db.run)
        thread.start()

    try:
        print(f"Serving on {args.host}:{args.port}")
        httpd.safe_start()
    finally:
        db.stop()
