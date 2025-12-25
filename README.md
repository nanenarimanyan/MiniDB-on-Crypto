# MiniDB – A Scalable Data Management System

This repository contains the implementation of **MiniDB**, a scalable, data-structure-driven management system designed to handle large datasets efficiently. The system provides core database operations, efficient indexing, and specialized graph-based analytics.

## I. Objective

The primary objective of MiniDB is to design and implement a lightweight in-memory database capable of performing search, insertion, deletion, modification, and range queries. This system applies advanced data structures, specifically AVL trees and graph algorithms, to ensure both functionality and efficiency.

---

## II. System Design and Data Structures

MiniDB is structured around three core modules: `CryptoDB` (Storage & Indexing), `CryptoGraph` (Graph Utilities), and `QueryEngine` (Interface).

### A. Storage and Indexing (`core/storage.py`)

The main data management is handled by the `CryptoDB` class.

- **Raw Storage:** Records are stored in memory using a Python list called `data_store`, allowing for O(1) record retrieval based on its unique ID.
- **Indexing Structure:** Efficient data lookup and range queries are implemented using AVL Tree Maps (`AVLTreeMap`). AVL Trees were chosen for indexing to provide O(log n) lookups/insertions/deletions and efficient range query performance.
- **Indexes Implemented:**
  - `timestamp_index`: Primary index mapping timestamps (converted to epoch seconds) to record IDs.
  - `token_index`: Secondary index for fast lookups based on the cryptocurrency token.
  - `wallet_from_index`: Secondary index for fast lookups based on the sender wallet address.

### B. Graph Features (`Graph.py`)

The system supports specialized graph-based analysis tailored for the transaction dataset. The `CryptoGraph` class builds and maintains this structure.

- **Nodes:** Wallet addresses.
- **Edges:** Directed edges from `wallet_from` -> `wallet_to`. Each edge stores statistics, including the transaction count and the total volume transferred.
- **Graph Algorithms & Analytics:** The system implements meaningful graph operations and analytical operations:
  - **Traversal:** Implements Breadth-first traversal (`bfs`) and Depth-first traversal (`dfs`) starting from a specified wallet.
  - **Neighbors:** The `neighbors` method returns the adjacent wallets and the associated edge statistics.
  - **Top-K Analytics:** The `top_tokens_for_wallet` method returns the top-K tokens by volume associated with a given wallet.

### C. Query Interface (`query_engine.py`)

The `QueryEngine` serves as an interface layer that orchestrates compound queries, filters, ranges, and graph lookups on top of the core `CryptoDB`.

- **Supported Queries:**
  - `range_by_time`: Retrieves records within a specified timestamp range by utilizing `range_query_by_timestamp` in `CryptoDB`.
  - `by_token`: Retrieves records matching a specific cryptocurrency token using the secondary token index.
  - `by_sender`: Retrieves records sent from a specific wallet address using the secondary sender wallet index.

---

## III. Usage & Core Operations

The system is designed to manage records based on a cryptocurrency transaction dataset schema, which typically includes fields like timestamp, token (or crypto_symbol), volume (or amount_usd/fee_usd), `wallet_from` (or sender_wallet), and `wallet_to` (or receiver_wallet).

### Data Management (Core Operations)

The `CryptoDB` supports the required core operations:

1.  **Ingestion (`ingest_data`):** Data is loaded from CSV files (e.g., `Dataset.csv`), parsing and cleaning fields before storing and indexing. Timestamps are converted to epoch seconds for indexing.
2.  **Insertion (`insert_record`):** Adds a new record to the `data_store` and updates the `timestamp_index`, `token_index`, and `wallet_from_index`.
3.  **Deletion (`delete_record`):** Removes the record's pointers from all indexes and replaces the record in the `data_store` with a tombstone (`None`) to maintain a stable ID space.
4.  **Update/Modification (`update_record`):** Handles changes to a record; if indexed fields (timestamp, token, or `wallet_from`) are modified, the old index entries are removed and new ones are added.

### Querying Data

Data querying is handled through the `QueryEngine` interface:

1.  **Range Queries:** Use `range_by_time(start_ts, end_ts)` to retrieve records within a time range, leveraging the AVL tree's `sub_map` function.
2.  **Indexed Lookups:** Query records efficiently by token or sender wallet using `by_token(token)` or `by_sender(wallet)`.
3.  **Graph Analysis:** Instantiate `CryptoGraph` and run traversal (e.g., `bfs('W000375')`) or specialized analytics (e.g., `top_tokens_for_wallet('W000375')`).

---

## IV. Requirements and Setup

To run MiniDB, ensure you have the following installed:

- **Python 3.x** (Python 3.8+ recommended)
- **Standard Libraries:** The project relies primarily on Python's standard libraries (`csv`, `collections`, `time`, `datetime`). No external heavy database drivers are required.

### How to Run the App

1.  **Clone the Repository:**

    ```bash
    git clone <repository_url>
    cd MiniDB
    ```

2.  **Prepare Data:**
    Ensure your `Dataset.csv` file is placed in the root directory (or update the file path in the main script).

3.  **Execute the Script:**
    Run the main entry point to ingest data and perform queries:
    ```bash
    python main.py
    ```

---

## V. Project Team

This project was designed and implemented by:

- **Nane Narimanyan**
- **Lina Mejlumyan**
- **Arpi Gyulgyulyan**
- **Lyudmila Zakaryan**
