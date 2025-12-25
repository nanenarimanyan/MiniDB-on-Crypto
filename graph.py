"""
Basic wallet-to-wallet graph utilities built from CryptoDB records.

Nodes: wallet addresses.
Edges: directed from wallet_from -> wallet_to with counts and total volume.
"""

from collections import defaultdict, deque
from typing import Dict, Any, List, Set


# ------------------ Graph ------------------
class CryptoGraph:
    def __init__(self):
        self.graph: Dict[str, Dict[str, Dict[str, float]]] = defaultdict(dict)
        self.wallet_token_counts = defaultdict(lambda: defaultdict(int))
        self.wallet_token_volume = defaultdict(lambda: defaultdict(float))

    def build_graph_from_db(self, db) -> None:
        """
        Build/reset the graph using records from a CryptoDB instance.
        Assumes records use fields: wallet_from, wallet_to, token, volume.
        """
        self.graph.clear()
        self.wallet_token_counts.clear()
        self.wallet_token_volume.clear()

        for rec in db.data_store:
            if rec is None:
                continue

            u = rec.get("wallet_from")
            v = rec.get("wallet_to")
            token = (rec.get("token") or "").strip()
            volume = float(rec.get("volume", 0.0))

            if not u or not v:
                continue

            self.wallet_token_counts[u][token] += 1
            self.wallet_token_counts[v][token] += 1
            self.wallet_token_volume[u][token] += volume
            self.wallet_token_volume[v][token] += volume

            if v not in self.graph[u]:
                self.graph[u][v] = {"count": 0, "total_volume": 0.0}

            self.graph[u][v]["count"] += 1
            self.graph[u][v]["total_volume"] += volume

    def neighbors(self, node: str) -> Dict[str, Dict[str, float]]:
        """Return neighbors and edge stats for a wallet."""
        return self.graph.get(node, {})

    def bfs(self, start: str) -> List[str]:
        """Breadth-first traversal from a start wallet."""
        if start not in self.graph:
            return [start]

        visited: Set[str] = {start}
        queue = deque([start])
        order: List[str] = []

        while queue:
            u = queue.popleft()
            order.append(u)
            for v in self.graph.get(u, {}):
                if v not in visited:
                    visited.add(v)
                    queue.append(v)
        return order

    def dfs(self, start: str) -> List[str]:
        """Depth-first traversal from a start wallet."""
        visited: Set[str] = set()
        order: List[str] = []

        def _dfs(u: str) -> None:
            visited.add(u)
            order.append(u)
            for v in self.graph.get(u, {}):
                if v not in visited:
                    _dfs(v)

        _dfs(start)
        return order

    def top_tokens_for_wallet(self, wallet: str, top_k: int = 3) -> List[Any]:
        """Return top-k tokens by volume for a wallet."""
        stats = self.wallet_token_volume.get(wallet, {})
        return sorted(stats.items(), key=lambda kv: kv[1], reverse=True)[:top_k]
