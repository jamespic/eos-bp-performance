import copy
import math
import random
import unittest
from .stats import Stats


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
