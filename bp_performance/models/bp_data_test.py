import unittest
from .bp_data import BpData


class BpDataTest(unittest.TestCase):
    def test_bp_data(self):
        instance = BpData()
        instance.process_block({
            'transactions': [
                {
                    'cpu_usage_us': 600,
                    'trx': {
                        'transaction': {
                            'actions': [
                                {
                                    'account': 'testtesttest',
                                    'name': 'testmethod'
                                }
                            ]
                        }
                    }
                }
            ]
        })
        instance.miss_block()
        self.assertEqual(instance.blocks_produced, 1)
        self.assertEqual(instance.slots_passed, 2)
        self.assertEqual(instance.tx_data['testtesttest:testmethod'].mean, 600)

    # FIXME: Add test for addition and subtraction


if __name__ == '__main__':
    unittest.main()
