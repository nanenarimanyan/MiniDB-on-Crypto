"""
Query engine placeholder.

Use CryptoDB for storage/indexing. This module can orchestrate
compound queries (filters, ranges, graph lookups) on top of the core DB.
"""

from typing import Iterable, Dict, Any


# ------------------ Query Engine ------------------
class QueryEngine:
    def __init__(self, db):
        self.db = db

    def range_by_time(self, start_ts: int, end_ts: int) -> Iterable[Dict[str, Any]]:
        return self.db.range_query_by_timestamp(start_ts, end_ts)

    def by_token(self, token: str) -> Iterable[Dict[str, Any]]:
        return self.db.get_records_by_token(token)

    def by_sender(self, wallet: str) -> Iterable[Dict[str, Any]]:
        return self.db.get_records_by_wallet_sender(wallet)
