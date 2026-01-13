# ScarDB: A High-Performance RDBMS Engine from Scratch in Rust

**ScarDB** is a lightweight Relational Database Management System (RDBMS) implemented in **Rust**. It features a custom storage engine, a schema-driven binary serialization format, B-Tree indexing for optimized lookups, and a relational execution engine capable of performing Nested Loop Joins.

This project demonstrates the implementation of low-level database internals, moving from raw byte manipulation on disk to a high-level SQL interface.

## Key Features

- **Custom Pager-Based Storage**: Manages data in 4KB pages to optimize disk I/O.
- **Fixed-Length Binary Format**: Uses a schema-driven binary format for storage, ensuring $O(1)$ row access via offsets.
- **B-Tree Indexing**: Implements primary key constraints and optimized point-lookups using a memory-resident B-Tree index.
- **Query Optimizer**: A built-in planner that automatically switches from a "Full Table Scan" to an "Index Lookup" when filtering by Primary Key.
- **Relational Joins**: Supports `INNER JOIN` operations using a Nested Loop Join algorithm.
- **Full CRUD Support**: Supports `CREATE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, and `DROP TABLE`.
- **Advanced SQL Features**: Includes `AUTOINCREMENT` for primary keys and `WHERE` clause filtering.
- **Interactive REPL**: A full-featured CLI with command history.
- **Web Integration**: A REST API demo showing ScarDB used as a library to power a web application.


## Architecture

### 1. The Storage Layer (The Pager & Bitmask)
Data is stored in a dedicated `.db` file for each table. To manage space efficiently:
- **Pages**: The file is divided into 4096-byte blocks.
- **Bitmask Management**: Each page contains a 64-byte header with a bitmask. This allows ScarDB to track occupied vs. empty slots, ensuring that when a row is deleted, the space is immediately reclaimed for the next `INSERT`.

### 2. The Catalog (Metadata Persistence)
The `catalog.json` file acts as the database's "brain." It persists table schemas (column names, types, primary key flags) and sequences for `AUTOINCREMENT` counters.

### 3. The Execution Engine
The engine transforms SQL AST (Abstract Syntax Tree) into logical commands:
- **Index Optimization**: If a query filters on a Primary Key (e.g., `WHERE id = 5`), the engine bypasses the file scan and probes the B-Tree for the exact page and slot.
- **Joins**: Joins are handled by a Nested Loop Join. The engine iterates through the "Outer" table and matches records in the "Inner" table based on the join predicate.


## Technical Decisions & Trade-offs

- **Why Rust?**: I chose rust for its zero-cost abstractions and memory safety. It allowed for safe raw byte manipulation when serializing data for disk storage.
- **Why Fixed-Length Records?**: By requiring a max length for strings (`VARCHAR`), we ensure that every row in a table is the same size. This allows for extremely fast "In-Place Updates" and predictable offset math.
- **B-Tree Indexing**: I chose a memory-resident B-Tree (reconstructed on startup) to ensure $O(\log N)$ lookup performance while maintaining code simplicity for this challenge.


## Supported SQL Syntax

```sql
-- Table Creation
CREATE TABLE users (id INT PRIMARY KEY AUTOINCREMENT, name VARCHAR(20), active BOOLEAN);

-- Data Manipulation
INSERT INTO users (name, active) VALUES ('Oscar', true);
UPDATE users SET active = false WHERE name = 'Oscar';
DELETE FROM users WHERE id = 1;

-- Querying & Joining
SELECT * FROM users WHERE active = true;
SELECT * FROM users JOIN orders ON users.id = orders.user_id;

-- Cleanup
DROP TABLE users;
```


## How to Run

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (latest stable)

### 1. Interactive CLI (REPL)
```bash
cargo run --package cli
```

### 2. Web App Demo
```bash
cargo run --package web
```
Then, access the API at `http://localhost:3000`.

### 3. Running Tests
```bash
cargo test
```


## Credits & Acknowledgments
- **sqlparser-rs**: Used for parsing SQL strings into an AST.
- **rustyline**: Used for the interactive REPL interface.
- **axum/tokio**: Used for the web demonstration.
- **serde**: Used for catalog serialization.


## Word of Reflection
This challenge was a deep dive into the "magic" of databases. Implementing the pager and the bitmask was particularly enlightening, as it forced me to think about how data actually lives on a physical platter rather than just in memory. Even if not every SQL edge case is handled, the core engine demonstrates a robust understanding of relational theory and systems programming.
