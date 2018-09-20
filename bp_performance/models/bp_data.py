from collections import defaultdict
from .stats import Stats


class BpData:
    def __init__(self):
        self.tx_data = defaultdict(Stats)
        self.slots_passed = 0
        self.blocks_produced = 0

    def miss_block(self):
        self.slots_passed += 1

    def process_block(self, block):
        self.slots_passed += 1
        self.blocks_produced += 1
        for tx in block['transactions']:
            cpu = tx['cpu_usage_us']
            if isinstance(tx['trx'], dict):
                actions = tx['trx']['transaction']['actions']
                if len(actions) == 1:  # Ignore multi-action txs, for now
                    action = actions[0]
                    action_sig = f"{action['account']}:{action['name']}"
                    self.tx_data[action_sig].observe(cpu)

    def __add__(self, other):
        result = BpData()
        for sig, data in self.tx_data.items:
            result.tx_data[sig] += data
        for sig, data in other.tx_data.items:
            result.tx_data[sig] += data
        result.slots_passed = self.slots_passed + other.slots_passed
        result.blocks_produced = self.blocks_produced + other.blocks_produced

    def __sub__(self, other):
        result = BpData()
        for sig, data in self.tx_data.items:
            result.tx_data[sig] += data
        for sig, data in other.tx_data.items:
            result.tx_data[sig] -= data
        result.slots_passed = self.slots_passed - other.slots_passed
        result.blocks_produced = self.blocks_produced - other.blocks_produced
