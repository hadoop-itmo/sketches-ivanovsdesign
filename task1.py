import numpy as np
import mmh3
from typing import List
from tqdm import tqdm
import pandas as pd
import sys

import os

from utils import gen_uniq_seq

class BloomFilterNumpy:
    '''
    Bloom-filter using numpy bit array
    '''
    def __init__(self,
                 n: int):
        self.n = n
        self.bit_array = np.zeros(n, dtype=bool)  # Use a numpy array of booleans

    def _hash(self, s):
        # Generate a single hash value for the given input
        return mmh3.hash(s, 0) % self.n

    def put(self, s):
        # Set the bit corresponding to the hash value
        hash_value = self._hash(s)
        self.bit_array[hash_value] = True

    def get(self, s):
        # Check if the bit corresponding to the hash value is set
        hash_value = self._hash(s)
        return self.bit_array[hash_value]

    def size(self):
        # Count the number of set bits in the bit array
        return np.sum(self.bit_array)
    
def run(bf_sizes: List,
        set_sizes: List):
    
    sys.stdout.write('Generating test files')

    for set_size in set_sizes:
        gen_uniq_seq(name = str(set_size),
                    n_records = set_size)
        
    result_np = []

    for bf_size in bf_sizes:
        sys.stdout.write(f'bf_size: {bf_size}')
        for set_size in set_sizes:
            sys.stdout.write(f'set_size: {set_size}')

            bf = BloomFilterNumpy(n = bf_size)

            fp_count = 0

            with open(f'{set_size}') as file: 
                for line in file:
                    if bf.get(line):
                        fp_count += 1
                    bf.put(line)
                ones_count_int = bf.size()

            result_np.append(
                {
                    'bf_size' : bf_size,
                    'set_size' : set_size,
                    'fp_count' : fp_count,
                    'ones_count' : ones_count_int
                }
            )

    return result_np
    

if __name__ == '__main__':

    bf_sizes = [8, 64, 1024, 65536, 16777216]
    set_sizes = [5, 50, 500, 5000, 5000000]

    os.makedirs('results', exist_ok=True)
    
    result_np = run(bf_sizes=bf_sizes,
                    set_sizes=set_sizes)
    result_df = pd.DataFrame(result_np)
    result_df.to_csv('results/np_df.csv')
    print(result_df)