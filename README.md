---

# 📘 MiniDB — A Pure Python Metadata Database Engine

### Query Batch Job Metadata Using SQL + Python, Directly From Folder Structures

---

# 🌟 Overview

**MiniDB** is a pure Python, zero-dependency engine designed specifically for **reading and analyzing batch job metadata** stored in folder structures.
It automatically detects `metadata.json` files inside job-run folders, extracts structured metadata, and turns them into **queryable database tables**.

You get:

* **Python Query API** (chainable, SQL-like)
* **Advanced SQL Engine**
* **Automatic Table Detection**
* **JOIN / MULTI-JOIN**
* **Aggregations (SUM, AVG, MIN, MAX, COUNT)**
* **GROUP BY + HAVING**
* **Complex WHERE (AND / OR / NOT / parentheses)**
* **ORDER BY, LIMIT**
* **Multi-day consolidation**
* **Metadata-only reading (all other files ignored)**

MiniDB turns this:

```
batch_logs/
 ├── JobA_20240201_010203/
 │     ├── metadata.json
 │     ├── execution.log
 │     └── error.txt
 ├── JobA_20240202_020501/
 │     ├── metadata.json
 │     └── stdout.log
 └── JobA_20240203_030422/
       └── metadata.json
```

Into:

```
db.execution_info
db.inputs
db.stats
```

Each table contains rows from **all days**, merged automatically.

---

# 🏭 Why MiniDB? (Real-World Metadata Motivation)

Batch systems (Airflow, Azure Data Factory, Informatica, TWS, Autosys, Databricks) create new metadata every time a job runs:

```
jobName_timestamp/
    metadata.json
    *.log
    *.txt
    debug/
    temp/
```

Metadata typically contains:

* start_time / end_time
* status: SUCCESS / FAILED
* duration_sec
* files read
* data source
* rows processed
* pipeline stats
* lineage
* retry attempts

Over time, metadata folders grow:

* thousands of runs
* across many days
* across many environments

### ❗ Pain Points Solved by MiniDB

| Problem                                                 | MiniDB Solution                         |
| ------------------------------------------------------- | --------------------------------------- |
| Metadata scattered across folders                       | Auto-detects all `metadata.json`        |
| Tons of irrelevant files (`*.log`, `*.txt`, other JSON) | Ignores everything except metadata.json |
| Hard to join metadata across runs                       | JOIN / MULTI-JOIN built in              |
| Hard to run analytics                                   | SQL Engine                              |
| Hard to write custom transformations                    | Python Query Engine                     |
| Changing schema                                         | Schema-less, automatically adapts       |

---

# 📁 Metadata Folder Structure

MiniDB expects a structure like:

```
batch_logs/
 ├── JobA_20240201_000101/
 │     └── metadata.json
 ├── JobA_20240201_010101/
 │     └── metadata.json
 ├── JobA_20240201_020301/
 │     └── metadata.json
 ...
```

Inside each folder:

```
metadata.json  ← MiniDB reads ONLY this file
execution.log  ← ignored
error.txt      ← ignored
debug.json     ← ignored
*.tmp          ← ignored
```

---

# 🧰 Example metadata.json

```json
{
    "execution_info": {
        "start_time": "2024-02-01T01:00:00Z",
        "end_time": "2024-02-01T01:10:00Z",
        "status": "SUCCESS",
        "duration_sec": 600
    },
    "stats": {
        "rows_in": 500000,
        "rows_out": 499500
    },
    "inputs": {
        "files_read": 12,
        "source": "landing/finance/"
    }
}
```

MiniDB auto-creates 3 tables:

```
db.execution_info
db.stats
db.inputs
```

Each row includes:

```
iid = folder name (e.g., JobA_20240201_000101)
```

---

# ⚙️ Instantiating MiniDB

Your modified constructor:

```python
from minibd import FolderDB

db = FolderDB(base_path="batch_logs", base_metadeta="metadata.json")
```

To use a custom metadata filename:

```python
db = FolderDB("batch_logs", base_metadeta="job_metadata.json")
```

MiniDB ignores all other files.

---

# 📅 Multi-Day Metadata Example (Automatic Table Consolidation)

If your metadata folders look like:

```
batch_logs/
 ├── 2024-02-01_JobA/
 │     └── metadata.json
 ├── 2024-02-02_JobA/
 │     └── metadata.json
 ├── 2024-02-03_JobA/
 │     └── metadata.json
```

MiniDB automatically merges them into unified tables.

### ✔ execution_info table:

| iid             | start_time           | status  | duration_sec |
| --------------- | -------------------- | ------- | ------------ |
| 2024-02-01_JobA | 2024-02-01T01:00:00Z | SUCCESS | 600          |
| 2024-02-02_JobA | 2024-02-02T01:00:00Z | FAILED  | 180          |
| 2024-02-03_JobA | 2024-02-03T01:00:00Z | SUCCESS | 520          |

### ✔ stats table:

| iid             | rows_in | rows_out |
| --------------- | ------- | -------- |
| 2024-02-01_JobA | 500000  | 499500   |
| 2024-02-02_JobA | 150000  | 149800   |
| 2024-02-03_JobA | 800000  | 798500   |

### ✔ inputs table:

| iid             | files_read | source           |
| --------------- | ---------- | ---------------- |
| 2024-02-01_JobA | 18         | landing/finance/ |
| 2024-02-02_JobA | 10         | landing/finance/ |
| 2024-02-03_JobA | 22         | landing/finance/ |

This automatic consolidation requires **no extra code** — MiniDB handles it.

---

# 🔍 Python Query API

-     ` .all() ` return in list of json
-     ` .show() ` print on screen in row column format


## WHERE example

```python
db.execution_info.where(
    ("status", "=", "FAILED"),
    ("duration_sec", ">", 300)
).show()
```

## JOIN example

```python
db.execution_info.join(db.stats, on="iid").show()
```

## MULTI JOIN

```python
db.execution_info.multi_join([db.stats, db.inputs], on="iid").show()
```

## GROUP BY + HAVING

```python
db.execution_info \
  .group_by("status") \
  .having(("COUNT", ">", 10)) \
  .show()
```

---

# 🧠 SQL Engine (Advanced)

```python
db.sql("YOUR SQL QUERY HERE").show() #print row table
k=db.sql("YOUR SQL QUERY HERE").all() #returns in list of JSON
print(k)
```

## Failed sqls

```sql
db.sql("""
SELECT iid, duration_sec
FROM execution_info
WHERE status = 'FAILED'
""").show()
```

## Full join across metadata categories

```sql
db.sql("""
SELECT e.iid, e.status, s.rows_in, i.files_read
FROM execution_info e
JOIN stats s USING(iid)
JOIN inputs i USING(iid)
""").show()
```

## Grouping example

```sql
db.sql("""
SELECT status, COUNT(*)
FROM execution_info
GROUP BY status
""").show()
```

## Average duration per day

```sql
db.sql("""
SELECT start_time AS Date,
       AVG(duration_sec)
FROM execution_info
GROUP BY day
""").show()
```

---

# 🔒 File Ignoring Behavior

MiniDB **only reads** the metadata file specified by `base_metadeta`.

Ignored files in job folders:

| File Type               | Ignored? |
| ----------------------- | -------- |
| `*.log`                 | ✔        |
| `*.txt`                 | ✔        |
| `other-json-files.json` | ✔        |
| `debug/`                | ✔        |
| `temp/`                 | ✔        |
| `*.tmp`                 | ✔        |

MiniDB remains stable even in noisy production folders.

---

# 🧱 Architecture Diagram

```
FolderDB(base_path, base_metadeta)
 ├── Scan all job folders
 ├── Read ONLY metadata.json
 ├── Auto-generate tables (one per top-level key)
 └── Attach SQLParserAdvanced + TableQuery

TableQuery Engine
 ├── WHERE(), SELECT(), LIMIT()
 ├── ORDER BY()
 ├── JOIN(), MULTI-JOIN()
 ├── GROUP BY(), HAVING()
 └── all()

SQLParserAdvanced Engine
 ├── Tokenizer
 ├── Boolean/AND/OR/NOT parser
 ├── Parentheses support
 ├── Aggregation + GROUP BY + HAVING
 ├── JOIN USING()
 ├── ORDER BY, LIMIT
 └── Executes via TableQuery
```

---

# 🛣 Roadmap

Planned features:

* CREATE TABLE + INSERT via SQL
* DISTINCT
* Column qualification (`e.status`)
* Subquery support
* Query optimizer
* Parquet/CSV export
* Web dashboard for metadata browsing

---

# 🏁 Conclusion

MiniDB turns **folders of batch metadata** into a **queryable lightweight database**, without:

* Hive
* Spark
* MySQL
* Heavy warehousing tools

It is:

* Fast
* Simple
* Flexible
* Metadata-focused
* Production-friendly
* 100% Python

Perfect for:

* ETL/BATCH job monitoring
* Failure analytics
* Pipeline performance tuning
* Data engineering tooling
* Debugging historical job runs
* Metadata lineage exploration

---# metadb
# dbmeta
# dbmeta
