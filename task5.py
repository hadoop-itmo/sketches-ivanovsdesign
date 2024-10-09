from typing import List
import csv
from collections import defaultdict
import os
import tempfile

from utils import gen_grouped_seq
import os

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


def count_keys(file_path: str,
               threshold: int) -> dict:
    """
    Counts keys and saves it into temp files,
    if memory threshold `chunk_size` was exceeded
    """
    temp_dir = tempfile.mkdtemp()
    chunk_size = 60000  # Memory limit
    key_counts = defaultdict(int)
    temp_files = []

    with open(file_path, newline='') as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            key = row[0]
            key_counts[key] += 1
            
            if len(key_counts) >= chunk_size:
                temp_file = os.path.join(temp_dir, f'temp_{len(temp_files)}.csv')
                with open(temp_file, 'w', newline='') as tempf:
                    writer = csv.writer(tempf)
                    for k, count in key_counts.items():
                        writer.writerow([k, count])
                temp_files.append(temp_file)
                key_counts.clear()
    
    if key_counts:
        temp_file = os.path.join(temp_dir, f'temp_{len(temp_files)}.csv')
        with open(temp_file, 'w', newline='') as tempf:
            writer = csv.writer(tempf)
            for k, count in key_counts.items():
                writer.writerow([k, count])
        temp_files.append(temp_file)
    
    # Aggregating values from temp files
    final_counts = defaultdict(int)
    for temp_file in temp_files:
        with open(temp_file, newline='') as tempf:
            reader = csv.reader(tempf)
            for row in reader:
                key, count = row
                final_counts[key] += int(count)
        os.remove(temp_file)
    
    os.rmdir(temp_dir)

    # Return keys which exceed the threshold
    return {k for k, count in final_counts.items() if count > threshold}

def find_common_heavy_keys(file1, file2, threshold):
    # Get 'exceeding' keys from each file
    heavy_keys_file1 = count_keys(file1, threshold)
    heavy_keys_file2 = count_keys(file2, threshold)
    
    # Find intersection - common keys
    common_heavy_keys = heavy_keys_file1.intersection(heavy_keys_file2)
    
    return common_heavy_keys


if __name__ == '__main__':

    pattern = [(10, 70000), (50, 30000)]  # 10 keys with 70,000 records each, 50 keys with 30,000 records
    gen_grouped_seq_fixed_keys("file1.csv", pattern, to_shuffle=True)
    gen_grouped_seq_fixed_keys("file2.csv", pattern, to_shuffle=True)

    file1 = 'file1.csv'
    file2 = 'file2.csv'
    threshold = 60000

    common_keys = find_common_heavy_keys(file1, file2, threshold)
    print(f"Keys exceeding {threshold} occurrences in both files: {common_keys}")
