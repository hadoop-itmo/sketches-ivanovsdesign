import math
import mmh3
from utils import gen_grouped_seq
from typing import List

class HyperLogLog:
    def __init__(self, b: int):
        self.b = b
        self.m = 1 << b  # m = 2^b
        self.registers = [0] * self.m
        self.alpha_m = self.get_alpha_m(self.m)

    def get_alpha_m(self, m):
        # Constants for different m values
        if m == 16:
            return 0.673
        elif m == 32:
            return 0.697
        elif m == 64:
            return 0.709
        else:
            return 0.7213 / (1 + 1.079 / m)

    def hash(self, value):
        # Hash the value using MurmurHash and convert to a binary string
        return mmh3.hash(value, signed=False)

    def rho(self, w):
        # Position of the leftmost 1-bit in the binary representation of w
        # Add 1 because we want the position starting from 1, not 0
        return len(w) - len(w.lstrip('0')) + 1

    def put(self, item):
        x = self.hash(item)
        # Convert the hash to binary and ensure it has enough bits
        x_bin = bin(x)[2:].zfill(32)  

        # Use the first b bits for the register index
        j = int(x_bin[:self.b], 2)
        
        # Use the remaining bits to calculate the rank (rho)
        w = x_bin[self.b:]
        self.registers[j] = max(self.registers[j], self.rho(w))

    def est_size(self):
        # Calculate the harmonic mean of 2^-M[j]
        Z = sum(2.0 ** -reg for reg in self.registers)
        E = self.alpha_m * self.m * self.m / Z

        # Apply small range correction
        if E <= 2.5 * self.m:
            V = self.registers.count(0)
            if V > 0:
                E = self.m * math.log(self.m / V)

        # Apply large range correction
        elif E > (1 / 30.0) * (1 << 32):
            E = -(1 << 32) * math.log(1 - E / (1 << 32))

        return E

def run_experiment(pattern: List,
                   filename: str,
                   true_size: int,
                   b: int):
    # Generate the dataset
    gen_grouped_seq(filename, pattern)

    # Initialize HyperLogLog
    hll = HyperLogLog(b=b)

    # Read the dataset and insert keys into HyperLogLog
    with open(filename, 'r') as f:
        for line in f:
            key = line.strip().split(':')[0]
            hll.put(key)

    # Estimate the size
    estimated_size = hll.est_size()
    
    # Calculate relative error
    relative_error = abs(estimated_size - true_size) / true_size

    print(f'Dataset: {filename}')
    print(f'True size: {true_size}')
    print(f'Estimated size: {estimated_size}')
    print(f'Relative error: {relative_error:.4%}\n')

if __name__ == '__main__':
    # Run experiments with different sizes and patterns
    run_experiment(pattern=[(500, 1), (10, 100)], filename="grouped_seq_500.txt", true_size=510, b=14)
    run_experiment(pattern=[(40000, 1), (100, 100)], filename="grouped_seq_50000.txt", true_size=40100, b=14)
    run_experiment(pattern=[(4000000, 1), (1000, 1000)], filename="grouped_seq_5000000.txt", true_size=4001000, b=18)
