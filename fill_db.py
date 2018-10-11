from bp_performance.models import Database

if __name__ == '__main__':
    db = Database('/tmp/tempdb', 'http://localhost:8888')
    db.run()
