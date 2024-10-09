from typing import Callable, List
import csv
import os 
import uuid

from task3 import CountingBloomFilter
from task4 import HyperLogLog

from utils import gen_uniq_seq

def process_file(file_path: str,
                 bloom_filter: Callable,
                 hll: Callable) -> None:
    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            key = row[0].strip()
            hll.put(key)
            bloom_filter.put(key)

def estimate_join_size(file1, file2):
    # Initialize HyperLogLog for estimating unique keys
    hll1 = HyperLogLog(b=14)
    hll2 = HyperLogLog(b=14)
    
    # Initialize Counting Bloom Filters
    bloom_filter1 = CountingBloomFilter(k=4, n=1000000, cap=4)
    bloom_filter2 = CountingBloomFilter(k=4, n=1000000, cap=4)
    
    # Process both files
    process_file(file1, bloom_filter1, hll1)
    process_file(file2, bloom_filter2, hll2)
    
    # Estimate unique counts
    unique_count1 = hll1.est_size()
    unique_count2 = hll2.est_size()
    
    # If both have less than 10k unique keys, calculate exact intersection
    if unique_count1 <= 10_000 and unique_count2 <= 10_000:
        keys1 = set()
        keys2 = set()
        with open(file1, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                keys1.add(row[0].strip())
        
        with open(file2, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                keys2.add(row[0].strip())
        
        exact_intersection = keys1.intersection(keys2)
        print(f"Exact intersection: {len(exact_intersection)}")
        return len(exact_intersection)
    
    # Estimate intersection size using Bloom Filters
    intersection_estimate = 0
    with open(file1, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            key = row[0].strip()
            if bloom_filter2.get(key):
                intersection_estimate += 1

    # Determine if the estimated intersection size suggests a large join
    if intersection_estimate > 100_000:
        return "> 100,000 (large join)"
    
    print(f"Estimated join size: {intersection_estimate}")
    return f"Estimated join size: {intersection_estimate}"

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

def run_experiments():
    # Experiment 1: Files with less than 10k million unique keys for exact intersection
    gen_uniq_seq("file1_exact.csv", 5000)
    gen_uniq_seq("file2_exact.csv", 4500)
    print("Experiment 1: Exact Intersection Test")
    print(estimate_join_size("file1_exact.csv", "file2_exact.csv"))

    # Experiment 2: Non-intersecting sets with high confidence of zero intersection
    gen_uniq_seq("file1_non_intersect.csv", 100000)
    gen_uniq_seq("file2_non_intersect.csv", 101000)
    print("Experiment 2: Non-Intersecting Sets Test")
    print(estimate_join_size("file1_non_intersect.csv", "file2_non_intersect.csv"))

    # Experiment 3: Large join size detection (threshold 100,000)
    shared_keys_large = [str(uuid.uuid4()) for _ in range(105_000)]
    gen_shared_keys("file1_large_join.csv", "file2_large_join.csv", shared_keys_large, 50_000, 50_000)
    print("Experiment 3: Large Join Detection Test")
    print(estimate_join_size("file1_large_join.csv", "file2_large_join.csv"))

    # Experiment 4: Reasonably accurate estimation for moderate intersection
    shared_keys_moderate = [str(uuid.uuid4()) for _ in range(40_000)]
    gen_shared_keys("file1_moderate.csv", "file2_moderate.csv", shared_keys_moderate, 40_000, 40_000)
    print("Experiment 4: Moderate Intersection Estimation Test")
    print(estimate_join_size("file1_moderate.csv", "file2_moderate.csv"))

    # Clean up generated files
    os.remove("file1_exact.csv")
    os.remove("file2_exact.csv")
    os.remove("file1_non_intersect.csv")
    os.remove("file2_non_intersect.csv")
    os.remove("file1_large_join.csv")
    os.remove("file2_large_join.csv")
    os.remove("file1_moderate.csv")
    os.remove("file2_moderate.csv")

if __name__ == '__main__':
    run_experiments()