from typing import List
import csv
import mmh3
from collections import defaultdict

import uuid
import random


# Modify the key generation to ensure overlap
def gen_grouped_seq_fixed_keys(name: str,
                               pattern: List,
                               *,
                               n_extra_cols: int = 0,
                               to_shuffle: bool = False):
    '''
    Generates keys without uuid
    '''
    def gen():
        num = 0
        for n_keys, n_records in pattern:
            for i1 in range(n_keys):
                # Fixed key for ensuring overlap
                body = f"key{i1 + num}"
                for i2 in range(n_records):
                    for j in range(n_extra_cols):
                        body += f",{uuid.uuid4()}"
                    yield body
            num += n_keys

    if to_shuffle:
        data = list(gen())
        random.shuffle(data)
        result = data
    else:
        result = gen()

    with open(name, "wt") as f:
        for v in result:
            print(v, file=f)



def hash_key(key: int,
             num_buckets: int) -> int:
    '''
    Hash the key using murmurhash.
    '''
    return mmh3.hash(key) % num_buckets

def count_keys(file_path: str,
               num_buckets: int) -> int:
    '''
    Count occurrences of keys in the file using hashing.
    '''
    counts = defaultdict(int)
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            key = row[0]
            bucket = hash_key(key, num_buckets)
            counts[bucket] += 1
    return counts

def find_problematic_keys(file1_path: str,
                          file2_path: str,
                          num_buckets: int,
                          threshold: int) -> set:
    '''
    Find keys that have more than 60000 occurrences in both files.
    '''
    # First pass: Count keys in both files
    counts1 = count_keys(file1_path, num_buckets)
    counts2 = count_keys(file2_path, num_buckets)
    
    # Second pass: Identify problematic keys
    problematic_keys = set()
    for bucket in counts1:
        if counts1[bucket] > threshold and counts2.get(bucket, 0) > threshold:
            problematic_keys.add(bucket)
    
    return problematic_keys


if __name__ == '__main__':
    
    # Constants
    num_buckets = 1000000  # Number of buckets for hashing
    threshold = 60000      # Threshold for problematic keys

    # Case 1. Regular behaviour. 10 keys exceed the threshold
    
    pattern = [(10, 70000), (50, 30000)]  # 10 keys with 70,000 records each, 50 keys with 30,000 records
    gen_grouped_seq_fixed_keys("file1.csv", pattern, to_shuffle=True)
    gen_grouped_seq_fixed_keys("file2.csv", pattern, to_shuffle=True)

    file1 = 'file1.csv'
    file2 = 'file2.csv'

    common_keys = find_problematic_keys(file1, file2, num_buckets, threshold)
    print(f"Regular case, 10 exceeding case:\nKeys exceeding {threshold} occurrences in both files: {common_keys}")
    
    # Case 2. All keys are unique
    
    pattern = [(100000, 1), (100000, 1)] 
    gen_grouped_seq_fixed_keys("file1.csv", pattern, to_shuffle=True)
    gen_grouped_seq_fixed_keys("file2.csv", pattern, to_shuffle=True)

    file1 = 'file1.csv'
    file2 = 'file2.csv'

    common_keys = find_problematic_keys(file1, file2, num_buckets, threshold)
    print(f"All unique case:\nKeys exceeding {threshold} occurrences in both files: {common_keys}")
    
    # Case 3. One key has 100k records, others are unique

    pattern = [(1, 100000), (100000, 1)] 
    gen_grouped_seq_fixed_keys("file1.csv", pattern, to_shuffle=True)
    gen_grouped_seq_fixed_keys("file2.csv", pattern, to_shuffle=True)

    file1 = 'file1.csv'
    file2 = 'file2.csv'

    common_keys = find_problematic_keys(file1, file2, num_buckets, threshold)
    print(f"All unique case:\nKeys exceeding {threshold} occurrences in both files: {common_keys}")
    