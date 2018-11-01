from pathlib import Path
from bp_performance.models import Database

if __name__ == '__main__':
    db = Database(str(Path.home() / 'bp-perf-db'), 'http://localhost:8888')
    db.run()
