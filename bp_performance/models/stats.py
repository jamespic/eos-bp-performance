import math
import renard

timing_buckets = list(renard.rrange(renard.R10, 100, 1000000))


class Stats:
    def __init__(self):
        self.measurements = [0] * len(timing_buckets)
        self.count = 0
        self.sum = 0
        self.sum_sq = 0

    def observe(self, x):
        self.count += 1
        self.sum += x
        self.sum_sq += x * x
        for i, bucket in zip(
                range(len(timing_buckets) - 1, -1, -1),
                reversed(timing_buckets)):
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
                    return timing_buckets[0]
                else:
                    prev_obs = self.measurements[i - 1]
                    x = (c - prev_obs) / (observations - prev_obs)
                    return x * timing_buckets[i] + (1 - x) * timing_buckets[i - 1]
        else:
            return timing_buckets[-1]

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
