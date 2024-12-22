from typing import Callable, List
import csv
import os 
import uuid

from task2 import KBloomFilterNumpy

from utils import gen_uniq_seq

import csv
import mmh3
from pybloom_live import BloomFilter
from collections import defaultdict, Counter

import time

def gen_shared_keys(file1_path: str,
                    file2_path: str,
                    shared_keys: List,
                    unique_keys1: int,
                    unique_keys2: int) -> None:
    """Generate two files with shared keys and unique keys."""
    with open(file1_path, "w") as f1, open(file2_path, "w") as f2:
        for key in shared_keys:
            f1.write(f"{key}\n")
            f2.write(f"{key}\n")

        for _ in range(unique_keys1):
            f1.write(f"{uuid.uuid4()}\n")

        for _ in range(unique_keys2):
            f2.write(f"{uuid.uuid4()}\n")

def read_csv_keys(file_path):
    keys = []
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                keys.append(row[0])
    return keys

def estimate_join_size(file1: str,
                       file2: str) -> int:
    # Parameters for Bloom Filter and Count-Min Sketch
    bloom_filter_size = 10**8  
    bloom_filter_error_rate = 0.01
    bloom_filter_k = 4
    cm_depth = 4
    cm_width = 10**6

    # Initialize Bloom Filter
    bloom_filter = BloomFilter(capacity=bloom_filter_size, error_rate=bloom_filter_error_rate)
    #bloom_filter = KBloomFilterNumpy(n=bloom_filter_size, k=bloom_filter_k)

    # Initialize Count-Min Sketch for file1
    cm_sketch_file1 = defaultdict(int)

    # Read file1 and populate Bloom Filter and Count-Min Sketch
    keys_file1 = read_csv_keys(file1)
    unique_keys_file1 = set()
    for key in keys_file1:
        bloom_filter.add(key)
        hash_val = mmh3.hash(key)
        cm_sketch_file1[hash_val % (cm_width * cm_depth)] += 1
        unique_keys_file1.add(key)

    # Check if we can switch to exact counting
    if len(unique_keys_file1) <= 10**6:
        # Exact counting for file1
        counter_file1 = Counter(keys_file1)

    # Initialize Count-Min Sketch for file2
    cm_sketch_file2 = defaultdict(int)

    # Read file2 and populate Count-Min Sketch
    keys_file2 = read_csv_keys(file2)
    unique_keys_file2 = set()
    for key in keys_file2:
        hash_val = mmh3.hash(key)
        cm_sketch_file2[hash_val % (cm_width * cm_depth)] += 1
        unique_keys_file2.add(key)

    # Check if we can switch to exact counting
    if len(unique_keys_file2) <= 10**6:
        # Exact counting for file2
        counter_file2 = Counter(keys_file2)

    # Estimate JOIN size
    join_size_estimate = 0
    if len(unique_keys_file1) <= 10**6 and len(unique_keys_file2) <= 10**6:
        # Exact counting for small files
        for key in counter_file1:
            if key in counter_file2:
                join_size_estimate += counter_file1[key] * counter_file2[key]
    else:
        # Probabilistic estimation for large files
        for key in keys_file2:
            if key in bloom_filter:  # Check if key is likely in file1
                hash_val = mmh3.hash(key)
                count_file1 = cm_sketch_file1[hash_val % (cm_width * cm_depth)]
                count_file2 = cm_sketch_file2[hash_val % (cm_width * cm_depth)]
                join_size_estimate += count_file1 * count_file2

                # If the estimate exceeds the threshold, stop and return a high estimate
                if join_size_estimate > 10**7:
                    return "JOIN size exceeds 10 million"

    return join_size_estimate

def run_experiments():
    # Experiment 1: Non-intersecting sets with high confidence of zero intersection
    gen_uniq_seq("file1_non_intersect.csv", 100000)
    gen_uniq_seq("file2_non_intersect.csv", 101000)
    print("Experiment: Non-Intersecting Sets Test")
    print(estimate_join_size("file1_non_intersect.csv", "file2_non_intersect.csv"))

    # Experiment 2:  Reasonably accurate estimation for moderate intersection 
    shared_keys_moderate = [str(uuid.uuid4()) for _ in range(1_100_000)]
    gen_shared_keys("file1_moderate.csv", "file2_moderate.csv", shared_keys_moderate, 100_000, 100_000)
    print("Experiment: Moderate size JOIN")
    print(estimate_join_size("file1_moderate.csv", "file2_moderate.csv"))

    # Experiment 3: Exact join
    shared_keys_exact = [str(uuid.uuid4()) for _ in range(40_000)]
    gen_shared_keys("file1_exact.csv", "file2_exact.csv", shared_keys_exact, 40_000, 40_000)
    print("Experiment: Exact size JOIN")
    print(estimate_join_size("file1_exact.csv", "file2_exact.csv"))
    
    # Experiment 4: Large join size detection (threshold 10 mil)
    shared_keys_large = [str(uuid.uuid4()) for _ in range(5_000_000)]
    gen_shared_keys("file1_large_join.csv", "file2_large_join.csv", shared_keys_large, 300_000, 300_000)
    print("Experiment: Large JOIN")
    print(estimate_join_size("file1_large_join.csv", "file2_large_join.csv"))

    # Clean up generated files
    os.remove("file1_non_intersect.csv")
    os.remove("file2_non_intersect.csv")
    os.remove("file1_large_join.csv")
    os.remove("file2_large_join.csv")
    os.remove("file1_moderate.csv")
    os.remove("file2_moderate.csv")
    os.remove("file1_exact.csv")
    os.remove("file2_exact.csv")
    

if __name__ == '__main__':
    start_time = time.time()
    run_experiments()
    print("--- %s seconds ---" % (time.time() - start_time))
    
    '''
    Expected output: 
    
    Experiment: Non-Intersecting Sets Test
    0
    Experiment: Moderate size JOIN
    2160236
    Experiment: Exact size JOIN
    40000
    Experiment: Large JOIN
    JOIN size exceeds 10 million
    --- 153.83575916290283 seconds ---
    '''