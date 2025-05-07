import time
import csv
import sys
from pathlib import Path

CSV_FILE_PATH = Path(__file__).resolve().parent.parent.parent
CSV_FILE_PATH = str(CSV_FILE_PATH / 'server' / 'dbcsv' / 'data' / 'schema4' / 'mock_data.csv')
start_time = time.time()
reader = csv.reader(open(CSV_FILE_PATH, "r", encoding="utf-8"))
res = []

for row in reader:
    res.append(row)

print(len(res))
time_taken = time.time() - start_time
print(f"Time taken to read CSV file: {time_taken:.2f} seconds")