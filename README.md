# 📘 DbMeta — A Pure Python Metadata Database Engine
### Query Batch Job Metadata Using SQL + Python, Directly From Folder Structures


[![PyPI version](https://img.shields.io/pypi/v/dbmeta.svg)](https://pypi.org/project/dbmeta/)
[![Python Versions](https://img.shields.io/pypi/pyversions/dbmeta.svg)](https://pypi.org/project/dbmeta/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🌟 Overview

**DbMeta** is a zero-dependency, pure Python engine engineered to inspect, consolidate, and query execution metadata scattered across nested directory structures.

In batch processing, ETL pipelines, and scheduled workflows (e.g., Airflow, Celery, Azure Data Factory, Databricks), each execution commonly creates a dedicated run directory containing operational logs, temporary files, and a JSON metadata payload. Gathering insights across hundreds of historical runs usually demands writing ad-hoc aggregation scripts or loading JSON dumps into heavyweight external databases.

DbMeta turns nested folder structures into an in-memory database automatically:
1. It traverses directory trees to locate your designated metadata JSON file.
2. It parses top-level JSON keys into independent queryable tables.
3. It merges all runs across folders and injects a folder identifier (`iid`) for relational joining.
4. It provides two query interfaces: a PySpark-style **Meta Query Engine** and a lightweight, AST-evaluating **Meta SQL Engine**.

---

## 📦 Installation

```bash
pip install dbmeta
```

DbMeta has zero third-party dependencies and runs natively on any modern Python 3 installation.

---

## 📂 How DbMeta Works

### Directory Structure & Metadata Files

DbMeta inspects immediate subdirectories inside a designated base path. While the default file is `metadata.json`, **any base JSON file name can be configured** (e.g., `run_metadata.json`, `execution_info.json`, `data.json`).

```text
batch_jobs/
 ├── run_20240201_001/
 │     ├── metadata.json          <-- Target metadata file
 │     ├── execution.log          <-- Ignored
 │     └── debug_dump.txt         <-- Ignored
 ├── run_20240202_002/
 │     ├── metadata.json          <-- Target metadata file
 │     └── error.log              <-- Ignored
 └── run_20240203_003/
       └── metadata.json          <-- Target metadata file
```

### Table Construction & The Injected `iid` Key

Inside each metadata JSON file, every top-level key represents an independent table.

Given a JSON file (`metadata.json`) containing:

```json
{
  "execution_info": {
    "status": "FAILED",
    "duration_sec": 420,
    "environment": "production"
  },
  "metrics": {
    "rows_processed": 140200,
    "error_count": 12
  }
}
```

DbMeta automatically:
* Dynamically registers two tables: `execution_info` and `metrics`.
* Maps nested attributes directly to column headers.
* Injects an **`iid`** column into every record matching the parent folder name (e.g., `"run_20240201_001"`).
* Unifies rows across all historical run directories into cohesive datasets.

---

## 🚀 Quick Start Tutorial

### Initialization

Import `FolderDB` and provide your base directory path and metadata target file name:

```python
from dbmeta import FolderDB

# Connect to the directory and load tables from custom-named JSON files
db = FolderDB(
    base_path="batch_jobs", 
    base_metadata="metadata.json"
)

# Inspect detected tables
print("Detected tables:", list(db.tables.keys()))
```

Detected tables are mapped directly as accessible attributes on the `db` instance (e.g., `db.execution_info`, `db.metrics`).

---

## 🐍 Engine 1: Meta Query Engine (PySpark-Style Python API)

The Meta Query Engine (`TableQuery`) provides a chainable, programmatic interface for filtering, projecting, and transforming metadata.

### Inspecting Results: `.show()` and `.all()`

```python
# Print formatted ASCII table to console
db.execution_info.show()

# Export query results as a list of Python dictionaries
records = db.execution_info.all()
```

### Column Selection: `.select()`

Restrict retrieved columns. Requesting non-existent column names raises an explicit `ValueError` showing available keys:

```python
# Project specific columns
db.execution_info.select("iid", "status", "duration_sec").show()

# Select all columns
db.execution_info.select("*").show()
```

### Filtering Data: `.where()`

Pass condition tuples in the format `(column_name, operator, comparison_value)`. Multiple tuples are evaluated using logical `AND`:

```python
db.execution_info.where(
    ("status", "=", "FAILED"),
    ("duration_sec", ">=", 300)
).show()
```

#### Supported Query Operators:

| Operator | Evaluation Type | Supported Types |
| :--- | :--- | :--- |
| `=` | Strict equality | Any |
| `!=` | Strict inequality | Any |
| `>`, `<` | Greater / Less than | Numeric (`int`, `float`) |
| `>=`, `<=` | Greater / Less or equal | Numeric (`int`, `float`) |
| `contains` | Case-insensitive substring match | Strings |
| `in` | Membership check in list/tuple | Collections |
| `not in` | Non-membership check | Collections |

```python
# Substring searching and set membership
db.execution_info.where(
    ("environment", "in", ["production", "staging"]),
    ("status", "contains", "fail")
).show()
```

### Sorting & Limiting: `.order_by()` and `.limit()`

```python
db.execution_info \
    .order_by("duration_sec", desc=True) \
    .limit(5) \
    .show()
```

### Relational Joins: `.join()` and `.multi_join()`

Combine tables using a common key, typically the injected `iid` column:

```python
# Join two tables
db.execution_info.join(db.metrics, on="iid").show()

# Multi-join three or more tables sequentially
db.execution_info.multi_join([db.metrics, db.pipeline_stats], on="iid").show()
```

### Grouping & Aggregations: `.group_by()` and `.having()`

Grouping organizes records by unique column values and automatically computes aggregates for every numeric attribute in the group:
* `COUNT`: Number of rows in group
* `SUM_<col>`: Sum of values for numeric column `<col>`
* `MIN_<col>`: Minimum value
* `MAX_<col>`: Maximum value
* `AVG_<col>`: Arithmetic mean

```python
# Group by environment and filter on computed COUNT
db.execution_info \
    .group_by("environment") \
    .having(("COUNT", ">", 5)) \
    .show()
```

---

## 🔍 Engine 2: Meta SQL Engine (Advanced SQL Parser)

DbMeta embeds `SQLParserAdvanced`, an internal SQL engine that parses queries into executable `TableQuery` operations using tokenization and a shunting-yard evaluation algorithm.

```python
# Execute SQL and display formatted ASCII output
db.sql("""
    SELECT iid, status, duration_sec
    FROM execution_info
    WHERE status = 'FAILED' AND duration_sec > 180
    ORDER BY duration_sec DESC
    LIMIT 10
""").show()

# Extract SQL output as Python dictionaries
failed_jobs = db.sql("SELECT iid, duration_sec FROM execution_info WHERE status = 'FAILED'").all()
```

### Supported SQL Syntax & Clauses

* **`SELECT`**: Project bare columns, wildcard `*`, or aggregate functions (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`). Supports output column renaming with `AS`.
* **`FROM`**: Target table name.
* **`JOIN ... USING(column)`**: Relational inner join on matching column names.
* **`WHERE`**: Boolean filters supporting `=`, `!=`, `>`, `<`, `>=`, `<=`, `CONTAINS`, `IN (...)`, `NOT IN (...)`, `AND`, `OR`, `NOT`, and parenthesized groups `()`.
* **`GROUP BY`**: Group records by one or more bare columns.
* **`HAVING`**: Filter grouped aggregations.
* **`ORDER BY`**: Sort output ascending (`ASC`) or descending (`DESC`).
* **`LIMIT`**: Truncate total rows returned.

---

## ⚠️ SQL Parser Rules & Syntax Constraints

DbMeta's SQL parser is intentionally lightweight and designed for fast, self-contained metadata analysis. To write queries that execute properly (or when configuring an LLM to generate queries for DbMeta), adhere strictly to the following syntax rules:

### 1. Bare Column Names Only (No Column Qualifiers)
Every column reference in `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, and `ORDER BY` must be a bare identifier. **Dot-notation qualifiers are not supported.**
* ✅ **Valid:** `SELECT iid, status, error_count FROM execution_info`
* ❌ **Invalid:** `SELECT execution_info.iid, execution_info.status FROM execution_info`
* ❌ **Invalid:** `WHERE execution_info.status = 'FAILED'`
* ❌ **Invalid:** `WHERE e.status = 'FAILED'`

### 2. No Table Aliases
Do not use table aliases in `FROM` or `JOIN` statements:
* ✅ **Valid:** `FROM execution_info JOIN metrics USING(iid)`
* ❌ **Invalid:** `FROM execution_info e JOIN metrics m USING(iid)`
* ❌ **Invalid:** `FROM execution_info AS e`

### 3. Joins Require `USING(col)`
Relational joins must specify a common join column using the `USING` keyword. The `ON` keyword and outer joins are not supported:
* ✅ **Valid:** `JOIN metrics USING(iid)`
* ❌ **Invalid:** `JOIN metrics ON execution_info.iid = metrics.iid`
* ❌ **Invalid:** `LEFT JOIN metrics USING(iid)`

### 4. Projection Aliases (`AS`) vs Filter Expressions
You can assign display aliases to projected fields and aggregates in the `SELECT` clause:
```sql
SELECT status, COUNT(*) AS total_runs, AVG(duration_sec) AS avg_duration
FROM execution_info
GROUP BY status
```
*Note: Column aliases define output labels only. Downstream clauses like `WHERE`, `ORDER BY`, or `HAVING` must reference existing raw column names or default aggregation names (e.g., `COUNT`, `SUM_duration_sec`).*

### 5. `IN` and `NOT IN` Accept Literal Values Only
Subqueries within `IN` expressions are unsupported. Lists must contain explicit literals:
* ✅ **Valid:** `WHERE status IN ('FAILED', 'ABORTED', 'TIMEOUT')`
* ❌ **Invalid:** `WHERE iid IN (SELECT iid FROM metrics)`

### 6. Single Statement Execution
Each SQL query must consist of exactly one `SELECT` statement. Subqueries, nested `SELECT` statements, `UNION`, `UNION ALL`, Common Table Expressions (`WITH ...`), and mutation statements (`INSERT`, `UPDATE`, `DELETE`) are not supported.

---

## 💡 Practical SQL Examples

### Multi-Table Join on Folder Key (`iid`)
```python
db.sql("""
    SELECT iid, status, rows_processed, error_count
    FROM execution_info
    JOIN metrics USING(iid)
    WHERE status = 'FAILED' AND error_count > 0
    ORDER BY error_count DESC
    LIMIT 10
""").show()
```

### Complex Boolean Expressions & Substring Matching
```python
db.sql("""
    SELECT iid, environment, duration_sec
    FROM execution_info
    WHERE (environment = 'production' AND duration_sec > 500)
       OR (environment = 'staging' AND status CONTAINS 'error')
""").show()
```

### Aggregations & Grouped Metrics
```python
db.sql("""
    SELECT environment, COUNT(*) AS total_jobs, AVG(duration_sec) AS avg_duration
    FROM execution_info
    GROUP BY environment
""").show()
```

---

## 🛡️ Robust File Handling & Noise Isolation

Production batch folders frequently accumulate unstructured operational artifacts. DbMeta isolates your metadata safely:

| File Type | Engine Behavior |
| :--- | :--- |
| Configured `base_metadata` (e.g., `metadata.json`) | **Parsed and converted into queryable tables** |
| Log files (`*.log`, `*.stdout`, `*.stderr`) | **Ignored** |
| Plain text and documentation (`*.txt`, `*.md`) | **Ignored** |
| Temporary or cache files (`*.tmp`, `.cache`) | **Ignored** |
| Secondary JSON dumps (e.g., `debug.json`) | **Ignored** |
| Nested subdirectories within run folders | **Ignored** |

If a specific folder contains invalid or corrupt JSON, DbMeta logs a warning and continues loading the remaining folders without failing the entire database initialization.

---

## 🏗️ Architecture Summary

```text
FolderDB (Directory Scanner & Registry)
 │
 ├── Discovers run folders in base_path
 ├── Reads ONLY base_metadata file
 ├── Injects 'iid' (folder name) into each record
 ├── Partitions top-level JSON keys into separate tables
 └── Exposes each table as a TableQuery attribute
        │
        ├── Engine 1: Meta Query Engine (TableQuery)
        │     ├── .where(), .select(), .order_by(), .limit()
        │     ├── .join(), .multi_join() using key
        │     ├── .group_by(), .having()
        │     └── .show() (ASCII Table) & .all() (Python dicts)
        │
        └── Engine 2: Meta SQL Engine (SQLParserAdvanced)
              ├── Regex clause extraction (SELECT, FROM, JOIN, WHERE, etc.)
              ├── Shunting-Yard tokenizer & RPN boolean evaluator
              └── Translates queries into executable TableQuery operations
```

---

## 🛣️ Roadmap

* [ ] Column qualification and identifier resolution (`table.column`, `alias.column`)
* [ ] `DISTINCT` keyword support in projection
* [ ] Direct Parquet and CSV export formats
* [ ] Standalone local web UI for visual metadata exploration
* [ ] Optional case-insensitive column resolution

---

## 📄 License

This project is licensed under the MIT License — see the LICENSE file for details.