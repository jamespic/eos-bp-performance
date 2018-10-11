import unittest
import copy
import math
import random

from .models import BpData, Stats

def tx_data(txs):
    return {
        'transactions': [
            {
                'cpu_usage_us': cpu_usage_us,
                'trx': {
                    'transaction': {
                        'actions': [
                            {
                                'account': account,
                                'name': name
                            }
                        ]
                    }
                }
            } for account, name, cpu_usage_us in txs
        ]
    }

class BpDataTest(unittest.TestCase):
    def test_bp_data(self):
        instance = BpData()
        instance.process_block(tx_data([('testtesttest', 'testmethod', 600)]), 1)
        instance.miss_block(2)
        self.assertEqual(instance.blocks_produced_total, 1)
        self.assertEqual(instance.slots_passed_total, 2)
        self.assertEqual(instance.tx_data['testtesttest:testmethod'].mean, 600)

        old_data = copy.deepcopy(instance)
        instance.process_block(tx_data([
            ('testtesttest', 'testmethod', 800),
            ('testtesttest', 'testmethod', 1000),
            ('testertester', 'method2', 100)
        ]), 3)
        diff = instance - old_data
        self.assertEqual(diff.blocks_produced_total, 1)
        self.assertEqual(diff.slots_passed_total, 1)
        self.assertEqual(diff.tx_data['testtesttest:testmethod'].mean, 900)
        self.assertEqual(diff.tx_data['testertester:method2'].mean, 100)


class StatsTest(unittest.TestCase):
    def test_stats(self):
        instance = Stats()
        for i in range(20000):
            instance.observe(random.uniform(100, 1000))
        self.assertAlmostEqual(instance.mean, 550.0, delta=5.0)
        self.assertAlmostEqual(instance.stddev, 900 / math.sqrt(12), delta=5.0)
        self.assertAlmostEqual(instance.median, 550.0, delta=5.0)
        self.assertAlmostEqual(instance.quantile(0.01), 109.0, delta=5.0)
        self.assertAlmostEqual(instance.quantile(0.99), 991.0, delta=5.0)
        instance2 = copy.deepcopy(instance)
        for i in range(20000):
            instance2.observe(random.uniform(1000, 10000))
        diff = instance2 - instance
        self.assertAlmostEqual(diff.mean, 5500.0, delta=50.0)
        self.assertAlmostEqual(diff.stddev, 9000 / math.sqrt(12), delta=50.0)
        self.assertAlmostEqual(diff.median, 5500.0, delta=50.0)
        self.assertAlmostEqual(diff.quantile(0.01), 1090.0, delta=50.0)
        self.assertAlmostEqual(diff.quantile(0.99), 9910.0, delta=50.0)


if __name__ == '__main__':
    unittest.main()
