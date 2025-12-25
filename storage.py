
import csv
import os
from datetime import datetime
from typing import Any, Dict, Optional, Iterable, List
from core.indexing import AVLTreeMap  # Import our AVL Tree implementation

class CryptoDB:
    # ------------------ Initialization ------------------
    
    def __init__(self):
        """Initializes the database with an empty data store and AVL index."""
        self.data_store: List[Dict[str, Any]] = []
        
        self.timestamp_index: AVLTreeMap = AVLTreeMap()

        self.token_index: AVLTreeMap = AVLTreeMap()
        self.wallet_from_index: AVLTreeMap = AVLTreeMap()
        
        self.next_record_id: int = 0
        self.active_records: int = 0

    # ------------------ Accessors ------------------
    def __len__(self) -> int:
        """Return the total number of records in the database."""
        return self.active_records

    # ------------------ Index helpers ------------------
    def _add_to_timestamp_index(self, timestamp: int, record_id: int) -> None:
        """Insert record_id into the timestamp index (support duplicates)."""
        current = self.timestamp_index.get(timestamp)
        if current is None:
            self.timestamp_index.put(timestamp, [record_id])
        else:
            bucket = current if isinstance(current, list) else [current]
            bucket.append(record_id)
            self.timestamp_index.put(timestamp, bucket)

    def _remove_from_timestamp_index(self, timestamp: int, record_id: int) -> None:
        """Remove record_id from the timestamp index; drop key if empty."""
        current = self.timestamp_index.get(timestamp)
        if current is None:
            return
        bucket = current if isinstance(current, list) else [current]
        try:
            bucket.remove(record_id)
        except ValueError:
            return

        if bucket:
            self.timestamp_index.put(timestamp, bucket)
        else:
            self.timestamp_index.remove(timestamp)

    def _add_to_index(self, index: AVLTreeMap, key: Any, record_id: int) -> None:
        """Generic helper to add record_id to a secondary AVL index."""
        if key is None:
            return
        current = index.get(key)
        if current is None:
            index.put(key, [record_id])
        else:
            bucket = current if isinstance(current, list) else [current]
            bucket.append(record_id)
            index.put(key, bucket)

    def _remove_from_index(self, index: AVLTreeMap, key: Any, record_id: int) -> None:
        """Generic helper to remove record_id from a secondary AVL index."""
        if key is None:
            return
        current = index.get(key)
        if current is None:
            return
        bucket = current if isinstance(current, list) else [current]
        try:
            bucket.remove(record_id)
        except ValueError:
            return
        if bucket:
            index.put(key, bucket)
        else:
            index.remove(key)

    # ------------------ Core mutations ------------------
    def insert_record(self, record: Dict[str, Any]) -> int:
        """Insert a single record and index it. Returns the new record ID."""
        if "timestamp" not in record:
            raise ValueError("Record must include a 'timestamp' field")

        record_ts = int(record["timestamp"])
        record_id = self.next_record_id
        self.next_record_id += 1

        self.data_store.append(record)
        self._add_to_timestamp_index(record_ts, record_id)
        self._add_to_index(self.token_index, record.get("token"), record_id)
        self._add_to_index(self.wallet_from_index, record.get("wallet_from"), record_id)
        self.active_records += 1
        return record_id

    def delete_record(self, record_id: int) -> bool:
        """Delete a record by ID; keeps ID space stable by leaving a tombstone."""
        if not (0 <= record_id < len(self.data_store)):
            return False
        record = self.data_store[record_id]
        if record is None:
            return False

        ts = int(record["timestamp"])
        token = record.get("token")
        wallet_from = record.get("wallet_from")
        self._remove_from_timestamp_index(ts, record_id)
        self._remove_from_index(self.token_index, token, record_id)
        self._remove_from_index(self.wallet_from_index, wallet_from, record_id)
        self.data_store[record_id] = None  # Tombstone to avoid shifting IDs
        self.active_records = max(0, self.active_records - 1)
        return True

    def update_record(self, record_id: int, updates: Dict[str, Any]) -> bool:
        """Update fields for a record ID; reindex if timestamp changes."""
        if not (0 <= record_id < len(self.data_store)):
            return False
        record = self.data_store[record_id]
        if record is None:
            return False

        old_ts = int(record.get("timestamp", 0))
        old_token = record.get("token")
        old_wallet_from = record.get("wallet_from")
        new_ts = int(updates.get("timestamp", old_ts))
        new_token = updates.get("token", old_token)
        new_wallet_from = updates.get("wallet_from", old_wallet_from)

        record.update(updates)
        record["timestamp"] = new_ts
        record["token"] = new_token
        record["wallet_from"] = new_wallet_from
        self.data_store[record_id] = record

        if new_ts != old_ts:
            self._remove_from_timestamp_index(old_ts, record_id)
            self._add_to_timestamp_index(new_ts, record_id)

        if new_token != old_token:
            self._remove_from_index(self.token_index, old_token, record_id)
            self._add_to_index(self.token_index, new_token, record_id)

        if new_wallet_from != old_wallet_from:
            self._remove_from_index(self.wallet_from_index, old_wallet_from, record_id)
            self._add_to_index(self.wallet_from_index, new_wallet_from, record_id)
        return True


    # ------------------ Data ingestion ------------------
    def _parse_timestamp(self, raw_ts: Any) -> int:
        """Convert a timestamp value (int or string) to epoch seconds for indexing."""
        if raw_ts is None:
            raise ValueError("Missing timestamp")
        if isinstance(raw_ts, (int, float)):
            return int(raw_ts)
        raw_str = str(raw_ts).strip()
        try:
            return int(raw_str)
        except ValueError:
            try:
                return int(datetime.fromisoformat(raw_str).timestamp())
            except ValueError:
                return int(datetime.strptime(raw_str, "%Y-%m-%d %H:%M:%S").timestamp())

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def ingest_data(self, file_path: str, key_column_name: str = 'timestamp') -> None:
        """
        Reads data from a CSV file, stores records, and builds the AVL index
        by the specified key column (default: 'timestamp').
        """
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}. Please check the 'data/' folder.")
            return

        print(f"Ingesting data from: {file_path}")
        total_records = 0
        
        try:
            with open(file_path, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)

                fieldnames = reader.fieldnames or []
                if key_column_name not in fieldnames:
                    print(f"Warning: key column '{key_column_name}' not found; available columns: {fieldnames}")

                for row in reader:
                    try:
                        raw_ts = row.get(key_column_name)
                        ts_epoch = self._parse_timestamp(raw_ts)

                        token = row.get('symbol') or row.get('crypto_symbol') or 'N/A'
                        price_val = row.get('price') or row.get('amount_usd')
                        volume_val = row.get('volume') or row.get('fee_usd')

                        record = {
                            "timestamp": ts_epoch,
                            "timestamp_raw": raw_ts,
                            "token": token,
                            "price": self._safe_float(price_val, 0.0),
                            "volume": self._safe_float(volume_val, 0.0),
                            "wallet_from": row.get('sender_wallet', 'N/A'),
                            "wallet_to": row.get('receiver_wallet', 'N/A'),
                            "status": row.get('status', 'N/A'),
                        }
                    except (KeyError, ValueError) as e:
                        continue

                    self.insert_record(record)
                    total_records += 1
                    
                    if total_records % 100000 == 0:
                        print(f"Progress: {total_records:,} records ingested...")

            print("--- Ingestion Summary ---")
            print(f"Total records ingested: {total_records:,}")
            print(f"Database list size: {len(self.data_store)}")
            print(f"AVL Index size: {len(self.timestamp_index)}")

        except Exception as e:
            print(f"An unexpected error occurred during ingestion: {e}")


    # ------------------ Core queries ------------------
    def get_record_by_timestamp(self, timestamp: int) -> Optional[Dict[str, Any]]:
        """
        Retrieves a single record by searching the AVL index for the timestamp.
        """
        record_ids = self.timestamp_index.get(timestamp)
        if record_ids is None:
            return None

        for rid in (record_ids if isinstance(record_ids, list) else [record_ids]):
            if 0 <= rid < len(self.data_store):
                record = self.data_store[rid]
                if record is not None:
                    return record
        return None

    def get_records_by_timestamp(self, timestamp: int) -> List[Dict[str, Any]]:
        """Return all records matching a timestamp (supports duplicates)."""
        matches: List[Dict[str, Any]] = []
        record_ids = self.timestamp_index.get(timestamp)
        if record_ids is None:
            return matches

        for rid in (record_ids if isinstance(record_ids, list) else [record_ids]):
            if 0 <= rid < len(self.data_store):
                record = self.data_store[rid]
                if record is not None:
                    matches.append(record)
        return matches

    def _collect_by_index(self, index: AVLTreeMap, key: Any) -> List[Dict[str, Any]]:
        """Shared collector for secondary indexes."""
        results: List[Dict[str, Any]] = []
        record_ids = index.get(key)
        if record_ids is None:
            return results
        for rid in (record_ids if isinstance(record_ids, list) else [record_ids]):
            if 0 <= rid < len(self.data_store):
                record = self.data_store[rid]
                if record is not None:
                    results.append(record)
        return results

    def get_records_by_token(self, token: str) -> List[Dict[str, Any]]:
        """Return all records for a given token using the token AVL index."""
        return self._collect_by_index(self.token_index, token)

    def get_records_by_wallet_sender(self, wallet_id: str) -> List[Dict[str, Any]]:
        """Return all records for a given sender wallet using the wallet AVL index."""
        return self._collect_by_index(self.wallet_from_index, wallet_id)

    def range_query_by_timestamp(self, start_ts: int, end_ts: int) -> Iterable[Dict[str, Any]]:
        """
        Performs a range query using the AVL index for timestamps k such that start_ts <= k < end_ts.
        Yields the full records from the data store.
        """
        for record_ids in self.timestamp_index.sub_map(start_ts, end_ts):
            ids_iterable = record_ids if isinstance(record_ids, list) else [record_ids]
            for record_id in ids_iterable:
                if 0 <= record_id < len(self.data_store):
                    record = self.data_store[record_id]
                    if record is not None:
                        yield record
        
