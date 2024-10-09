import numpy as np
import mmh3
from typing import List
from tqdm import tqdm
import pandas as pd
import sys
import os

from utils import gen_uniq_seq

class KBloomFilterNumpy:
    def __init__(self,
                 n: int,
                 k: int):
        """
        Bloom filter with k hash funcs
        implemented with numpy
        """
        self.n = n
        self.k = k
        self.bit_array = np.zeros(n, dtype=bool)

    def _hashes(self, item):
        return [mmh3.hash(item, seed) % self.n for seed in range(self.k)]

    def put(self, item):
        for hash_value in self._hashes(item):
            self.bit_array[hash_value] = True

    def get(self, item):
        return all(self.bit_array[hash_value] for hash_value in self._hashes(item))

    def size(self):
        return np.sum(self.bit_array) / self.k
    
def run(bf_sizes: List,
        set_sizes: List,
        k_values: List):
    
    sys.stdout.write('Generating test files')

    for set_size in set_sizes:
        gen_uniq_seq(name = str(set_size),
                    n_records = set_size)
        
    result_np_k = []

    for k in k_values:
        print(f'Experiment with {k} hash functions')
        for bf_size in tqdm(bf_sizes,
                            total=len(set_sizes),
                            position=0,
                            leave=False,
                            desc='Iterating through bf sizes'):
            for set_size in tqdm(set_sizes,
                                    total=len(set_sizes),
                                    position=1,
                                    leave=False,
                                    desc='Iterating through set sizes'):

                bf = KBloomFilterNumpy(n = bf_size,
                                        k = k)

                fp_count = 0

                with open(f'{set_size}') as file: 
                    for line in file:
                        if bf.get(line):
                            fp_count += 1
                        bf.put(line)
                    ones_count_int = bf.size()

                result_np_k.append(
                    {
                        'k' : k,
                        'bf_size' : bf_size,
                        'set_size' : set_size,
                        'fp_count' : fp_count,
                        'ones_count' : ones_count_int
                    }
                )
    

if __name__ == '__main__':
    
    k_values = [1, 2, 3, 4]
    bf_sizes = [8, 64, 1024, 65536, 16777216]
    set_sizes = [5, 50, 500, 5000, 5000000]

    os.makedirs('results', exist_ok=True)
    
    result_np = run(bf_sizes=bf_sizes,
                    set_sizes=set_sizes,
                    k_values=k_values)
    result_df = pd.DataFrame(result_np)
    result_df.to_csv('results/k_df.csv')
    
    print(result_df)