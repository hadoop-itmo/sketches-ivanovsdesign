import csv
from collections import defaultdict
import os
import tempfile

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
    
    file1 = 'file1.csv'
    file2 = 'file2.csv'
    threshold = 60000

    common_keys = find_common_heavy_keys(file1, file2, threshold)
    print(f"Keys exceeding {threshold} occurrences in both files: {common_keys}")
