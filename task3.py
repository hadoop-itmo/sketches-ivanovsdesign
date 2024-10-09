import numpy as np
import mmh3
from typing import List
from tqdm import tqdm
import pandas as pd
import sys

from utils import gen_uniq_seq

class CountingBloomFilter:
    def __init__(self,
                 k: int,
                 n: int,
                 cap: int):
        """
        Parameters:
        k (int): Number of hash functions.
        n (int): Number of counters.
        cap (int): Number of bits per counter.
        """
        self.k = k
        self.n = n
        self.cap = cap

        # Calculate the number of counters that fit into a 64-bit integer
        counters_per_int = 64 // cap
        self.counters_per_int = counters_per_int

        # Calculate how many 64-bit integers we need
        num_ints = (n + counters_per_int - 1) // counters_per_int
        self.num_ints = num_ints

        # Initialize the bit array
        self.bit_array = np.zeros(num_ints, dtype=np.uint64)

    def _hashes(self, item):
        """Generate k hash values for the item using different seeds."""
        return [mmh3.hash(item, seed) % self.n for seed in range(self.k)]

    def _get_counter_index_and_offset(self, hash_value):
        """Calculate the index and bit offset for the given hash value."""
        int_index = hash_value // self.counters_per_int
        bit_offset = (hash_value % self.counters_per_int) * self.cap
        return int_index, bit_offset

    def put(self, item):
        """Insert an item into the Counting Bloom Filter."""
        for hash_value in self._hashes(item):
            int_index, bit_offset = self._get_counter_index_and_offset(hash_value)
            # Extract the counter
            mask = (1 << self.cap) - 1
            current_count = (self.bit_array[int_index] >> bit_offset) & mask
            # Increment the counter if it is not at its maximum
            if current_count < mask:
                self.bit_array[int_index] += (1 << bit_offset)

    def get(self, item):
        """Check if an item is in the Counting Bloom Filter."""
        for hash_value in self._hashes(item):
            int_index, bit_offset = self._get_counter_index_and_offset(hash_value)
            # Extract the counter
            mask = (1 << self.cap) - 1
            current_count = (self.bit_array[int_index] >> bit_offset) & mask
            if current_count == 0:
                return False
        return True

    def size(self):
        """Return the sum of all counters divided by k."""
        mask = (1 << self.cap) - 1
        total_count = 0
        for int_index in range(self.num_ints):
            value = self.bit_array[int_index]
            for _ in range(self.counters_per_int):
                total_count += value & mask
                value >>= self.cap
        return total_count / self.k


def cap_experiment(cap: int,
                   k: int,
                   bf_size: int,
                   set_size: int) -> tuple[int, float]:
    """
    Runs experiment with cap bloom filter
    """
    gen_uniq_seq(f'cap_{set_size}.csv', 5000)
    counting_bloom_filter = CountingBloomFilter(cap=cap,
                                                k=k,
                                                n=bf_size)
    fp_count = 0

    with open(f'cap_{set_size}.csv') as file: 
        for line in file:
            if counting_bloom_filter.get(line):
                fp_count += 1
            counting_bloom_filter.put(line)
        ones_count = counting_bloom_filter.size()

    return fp_count, ones_count

def run_cap_experiments():
    """
    Defines experiments with counting Bloom Filter
    """
    experiments = [
        {"cap": 2, "k": 3, "bf_size": 65536, "set_size": 10000},
        {"cap": 5, "k": 2, "bf_size": 8192, "set_size": 1000},
        {"cap": 3, "k": 4, "bf_size": 32768, "set_size": 5000},
        {"cap": 4, "k": 5, "bf_size": 32768, "set_size": 2000},
        {"cap": 3, "k": 3, "bf_size": 16384, "set_size": 3000}  # 5-bit counter not directly specified in parameters
    ]

    results = []

    for exp in experiments:
        fp_count, ones_count = cap_experiment(
            cap=exp["cap"],
            k=exp["k"],
            bf_size=exp["bf_size"],
            set_size=exp["set_size"]
        )
        results.append({
            "cap": exp["cap"],
            "k": exp["k"],
            "bf_size": exp["bf_size"],
            "set_size": exp["set_size"],
            "fp_count": fp_count,
            "ones_count": ones_count
        })

    for result in results:
        print(f"Experiment: cap={result['cap']}, k={result['k']}, bf_size={result['bf_size']}, set_size={result['set_size']}")
        print(f"  False Positives: {result['fp_count']}, Ones in Filter: {result['ones_count']}")

    return results

if __name__ == '__main__':
    results = run_cap_experiments()
    results_df = pd.DataFrame(results)
    results_df.to_csv('cap_df.csv')
    print(results_df)

