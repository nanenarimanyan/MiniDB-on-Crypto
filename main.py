
import os
import time
from core.storage import CryptoDB

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(BASE_DIR, 'data', 'dataset.csv')

def run_ingest_and_smoke_test():
    print("--- CryptoDB ingest + smoke test ---")
    db = CryptoDB()

    start_time = time.time()
    db.ingest_data(CSV_FILE_PATH)
    end_time = time.time()

    print(f"Ingested {len(db)} records in {end_time - start_time:.2f}s")

    keys = list(db.timestamp_index)
    if not keys:
        print("No records loaded.")
        return

    mid_ts = keys[len(keys) // 2]
    record = db.get_record_by_timestamp(mid_ts)
    print(f"Sample GET at {mid_ts}: token={record.get('token')}, price={record.get('price')}")

    range_start = keys[0]
    range_end = range_start + 10
    records_in_range = list(db.range_query_by_timestamp(range_start, range_end))
    print(f"Range [{range_start}, {range_end}) -> {len(records_in_range)} records")
    for i, rec in enumerate(records_in_range[:3]):
        print(f"  - TS: {rec.get('timestamp')}, Token: {rec.get('token')}")
    if len(records_in_range) > 3:
        print("  ...")


if __name__ == "__main__":
    run_ingest_and_smoke_test()
