import datetime
import json
import lmdb
import math
import pickle
import renard
import traceback
import wrapt
import sys
import time
import yaml

from ciso8601 import parse_datetime
from collections import defaultdict
from multiprocessing.pool import ThreadPool
from urllib.request import urlopen
from urllib.error import URLError

_timing_buckets = list(renard.rrange(renard.R20, 100, 500000))


class Stats:
    def __init__(self):
        self.measurements = [0] * len(_timing_buckets)
        self.count = 0
        self.sum = 0
        self.sum_sq = 0

    def observe(self, x):
        self.count += 1
        self.sum += x
        self.sum_sq += x * x
        for i, bucket in zip(
                range(len(_timing_buckets) - 1, -1, -1),
                reversed(_timing_buckets)):
            if x < bucket:
                self.measurements[i] += 1
            else:
                break

    @property
    def mean(self):
        return self.sum / self.count

    @property
    def stddev(self):
        if self.count == 0:
            return float('nan')
        return math.sqrt(
            self.sum_sq / self.count
            - (self.sum / self.count) ** 2
        )

    @property
    def median(self):
        return self.quantile(0.5)

    def quantile(self, q):
        c = q * self.count  # how many values should be less than this
        for i, observations in enumerate(self.measurements):
            if observations > c or q == 1.0 and observations == c:
                # the c'th observation was in this bucket
                if i == 0:
                    return _timing_buckets[0]
                else:
                    prev_obs = self.measurements[i - 1]
                    x = (c - prev_obs) / (observations - prev_obs)
                    return x * _timing_buckets[i] + (1 - x) * _timing_buckets[i - 1]
        else:
            return _timing_buckets[-1]

    def __add__(self, other):
        result = Stats()
        result.measurements = [a + b for a, b in zip(self.measurements, other.measurements)]
        result.count = self.count + other.count
        result.sum = self.sum + other.sum
        result.sum_sq = self.sum_sq + other.sum_sq
        return result

    def __sub__(self, other):
        result = Stats()
        result.measurements = [a - b for a, b in zip(self.measurements, other.measurements)]
        result.count = self.count - other.count
        result.sum = self.sum - other.sum
        result.sum_sq = self.sum_sq - other.sum_sq
        return result


class BpData:
    def __init__(self):
        self.tx_data = defaultdict(Stats)
        self.slots_passed = [0] * 12
        self.blocks_produced = [0] * 12

    def miss_block(self, slot):
        self.slots_passed[slot] += 1

    def process_block(self, block, slot):
        self.slots_passed[slot] += 1
        self.blocks_produced[slot] += 1
        for tx in block['transactions']:
            cpu = tx['cpu_usage_us']
            if isinstance(tx['trx'], dict):
                actions = tx['trx']['transaction']['actions']
                if len(actions) == 1:  # Ignore multi-action txs, for now
                    action = actions[0]
                    action_sig = f"{action['account']}:{action['name']}"
                    self.tx_data[action_sig].observe(cpu)

    @property
    def slots_passed_total(self):
        return sum(self.slots_passed)

    @property
    def blocks_produced_total(self):
        return sum(self.blocks_produced)

    def __add__(self, other):
        result = BpData()
        for sig, data in self.tx_data.items:
            result.tx_data[sig] += data
        for sig, data in other.tx_data.items:
            result.tx_data[sig] += data
        result.slots_passed = [a + b for a, b in zip(self.slots_passed, other.slots_passed)]
        result.blocks_produced = [a + b for a, b in zip(self.blocks_produced, other.blocks_produced)]
        return result

    def __sub__(self, other):
        result = BpData()
        for sig, data in self.tx_data.items():
            result.tx_data[sig] += data
        for sig, data in other.tx_data.items():
            result.tx_data[sig] -= data
        result.slots_passed = [a - b for a, b in zip(self.slots_passed, other.slots_passed)]
        result.blocks_produced = [a - b for a, b in zip(self.blocks_produced, other.blocks_produced)]
        return result

def _timestamp_to_slot(timestamp):
    epoch_time = timestamp - datetime.datetime.fromtimestamp(946684800)
    return int(epoch_time.total_seconds() * 2)

def _block_producer_for_timestamp(timestamp, schedule):
    slot = _timestamp_to_slot(timestamp)
    return schedule[(slot % (len(schedule) * 12)) // 12], slot % 12


def _backoff():
    yield
    time.sleep(5)
    yield
    time.sleep(10)
    yield
    time.sleep(15)
    yield
    time.sleep(20)
    yield
    time.sleep(30)
    yield
    time.sleep(60)
    yield
    time.sleep(120)
    yield
    time.sleep(300)
    yield
    time.sleep(900)


@wrapt.decorator
def _with_backoff(wrapped, instance, args, kwargs):
    for _ in _backoff():
        try:
            return wrapped(*args, **kwargs)
        except URLError:
            traceback.print_exc()

class BlockSummary:
    def __init__(self):
        self.producers = defaultdict(BpData)
        self.last_block_num = None
        self.last_schedule_num = None


class Database:
    def __init__(self, db_path, nodeos_endpoint):
        self._endpoint = nodeos_endpoint
        self._db = lmdb.open(db_path, map_size=64 * 1024**3, max_dbs=2)
        self._block_db = self._db.open_db(b'block_db')
        self._schedule_db = self._db.open_db(b'schedule_db', integerkey=True)

    def run(self, starting_block=1):
        self._stopped = False
        self._init_block(starting_block)

        with ThreadPool(8) as executor:
            while not self._stopped:
                block_num = self._last_irreversible_block_number()
                if block_num != self.current_block.last_block_num:
                    # Limit to 1000 blocks at once
                    block_num = min(block_num, self.current_block.last_block_num + 1000)
                    print(
                        f"Fetching data for blocks {self.current_block.last_block_num + 1} to {block_num}",
                        file=sys.stderr)
                    for new_block in executor.imap(
                            self._get_block,
                            range(self.current_block.last_block_num + 1, block_num + 1)):
                        self._handle_block(new_block)
                else:
                    time.sleep(1.0)

    def _init_block(self, starting_block=1):
        with self._db.begin(self._block_db, write=True) as tx:
            with tx.cursor() as cursor:
                if cursor.last():
                    timestamp, block_data = cursor.item()
                    self.timestamp = parse_datetime(timestamp.decode('ascii'))
                    self.current_block = pickle.loads(block_data)
                else:
                    block_data = self._get_block(starting_block)
                    timestamp = parse_datetime(block_data['timestamp'])
                    block = BlockSummary()
                    block.last_block_num = starting_block
                    block.last_schedule_num = block_data['schedule_version']
                    self.timestamp = timestamp
                    self.current_block = block
                    cursor.put(
                        timestamp.isoformat().encode('ascii'),
                        pickle.dumps(block)
                    )

    def _handle_block(self, block):
        new_timestamp = parse_datetime(block['timestamp'])
        if block['new_producers'] is not None:
            self._save_schedule(block['new_producers'])

        # Fill in gaps in producer schedule
        last_timestamp = self.timestamp
        slots_missed = int((new_timestamp - last_timestamp).total_seconds() * 2) - 1
        for i in range(slots_missed):
            schedule = self._schedule(self.current_block.last_schedule_num)
            if schedule:
                missed_timestamp = last_timestamp + (i + 1) * datetime.timedelta(seconds=0.5)
                producer, slot_position = _block_producer_for_timestamp(missed_timestamp, schedule)
                self.timestamp = missed_timestamp
                self.current_block.producers[producer].miss_block(slot_position)
                self._maybe_save_block()

        schedule = self._schedule(block['schedule_version'])
        _, slot_position = _block_producer_for_timestamp(new_timestamp, schedule)
        self.current_block.producers[block['producer']].process_block(block, slot_position)
        self.current_block.last_schedule_num = block['schedule_version']
        self.current_block.last_block_num = block['block_num']
        self._maybe_save_block()
        self.timestamp = new_timestamp

    def _maybe_save_block(self):
        slot = _timestamp_to_slot(self.timestamp)
        if slot % (21 * 12) == 0:
            print(
                f"Saving block {self.current_block.last_block_num} at timestamp {self.timestamp}",
                file=sys.stderr
            )
            print(yaml.dump(self.current_block))
            with self._db.begin(self._block_db, write=True) as tx:
                tx.put(
                    self.timestamp.isoformat().encode('ascii'),
                    pickle.dumps(self.current_block),
                    db=self._block_db
                )

    def _save_schedule(self, schedule):
        with self._db.begin(self._schedule_db, write=True) as tx:
            tx.put(
                schedule['version'],
                pickle.dumps([producer['producer_name'] for producer in schedule['producers']])
            )

    def _schedule(self, schedule_num):
        if schedule_num is None:
            return None
        if schedule_num == 0:
            return ['eosio']
        else:
            with self._db.begin(self._schedule_db) as tx:
                data = tx.get(schedule_num)
                if data:
                    return pickle.loads(schedule)

    def stop(self):
        self._stopped = True

    @_with_backoff
    def _last_irreversible_block_number(self):
        info = json.loads(
            urlopen(
                f"{self._endpoint}/v1/chain/get_info"
            ).read().decode('utf-8', errors='replace')
        )
        return info['last_irreversible_block_num']

    @_with_backoff
    def _get_block(self, block):
        return json.loads(
            urlopen(
                f"{self._endpoint}/v1/chain/get_block",
                json.dumps({
                    "block_num_or_id": block
                }).encode('utf-8')
            ).read().decode('utf-8', errors='replace')
        )