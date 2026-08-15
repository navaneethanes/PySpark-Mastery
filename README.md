# 🔥 PySpark Advanced : Fundamentals to Interview Mastery
![PySpark](https://img.shields.io/badge/PySpark-Advanced-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-00ADD8?style=for-the-badge)
![Window Functions](https://img.shields.io/badge/Window-Functions-brightgreen?style=for-the-badge)
![Spark SQL](https://img.shields.io/badge/Spark-SQL-blue?style=for-the-badge)
![Interview Ready](https://img.shields.io/badge/Interview-Ready-violet?style=for-the-badge)

---

## 📌 Section Overview

This repository is a **two-part, hands-on record of learning PySpark** — going from core fundamentals to the exact kind of reasoning tested in real PySpark interviews.

- **`PySpark-Mastery.ipynb`** — PySpark built up from scratch: reading data, schemas, transformations, joins, window functions, UDFs, writing data, and Spark SQL.
- **`PySpark_Interview.dbc`** — 30+ real-world scenario-based coding problems (the kind asked in actual data engineering interviews) plus deep-dive write-ups on Spark internals: architecture, Catalyst optimizer, partitions, caching, Delta Lake, AQE, data skew, and more.

> The goal wasn't just "learning PySpark syntax" — it was building the ability to reason about *why* Spark behaves the way it does, and to solve realistic data engineering problems under interview conditions.

---

## 🎯 Aim & Objectives

- Understand **Spark's architecture** — Driver, Executors, Cluster Manager, Jobs/Stages/Tasks, and Lazy Evaluation
- Master **PySpark DataFrame transformations** from beginner to advanced level
- Work confidently with **Spark SQL, Managed/External tables, and multiple file formats**
- Solve **real-world data engineering scenarios**: deduplication, top-N per group, running totals, schema drift, corrupt records
- Understand **performance internals**: partitions, shuffles, caching, broadcast joins, data skew, and Adaptive Query Execution (AQE)
- Understand **Delta Lake's edge over plain file formats**: ACID transactions, schema enforcement, time travel, and `OPTIMIZE`/`ZORDER`
- Be fully prepared to explain and defend these concepts in a **technical interview setting**

---

## 🧰 Tech Stack & Concepts

| Concept | Purpose |
|---|---|
| PySpark DataFrame API | Core data manipulation |
| Spark SQL | SQL-based querying on DataFrames/tables |
| Window Functions | Ranking, running totals, partitioned analytics |
| User Defined Functions (UDF) | Custom row-level logic |
| Delta Lake | ACID transactions, MERGE/upsert, time travel |
| Databricks Utilities (`dbutils`) | File system operations in Databricks |
| Catalyst Optimizer / AQE | Query planning & runtime optimization |
| Broadcast Variables & Joins | Efficient small-table distribution |
| Partitioning & Z-Ordering | Physical data layout for faster reads |

---

## 🏗️ Learning Architecture

```
Data Reading & Schema Design
        ↓
Core Transformations (select, filter, withColumn, sort, drop)
        ↓
Intermediate Transformations (strings, dates, nulls, split/explode)
        ↓
Aggregations (groupBy, pivot, collect_list, when-otherwise)
        ↓
Joins & Window Functions
        ↓
User Defined Functions
        ↓
Data Writing & Spark SQL
        ↓
Real-World Interview Scenarios (30+ problems)
        ↓
Spark Internals & Performance Tuning (architecture, Catalyst, AQE, Delta Lake)
```

---

# 📘 Notebook 1 — PySpark Fundamentals (`1_Tutorial.ipynb`)

## 🧩 Topic Breakdown

| Section | Key Concepts Practiced |
|---|---|
| Data Reading | JSON/CSV reading, `dbutils.fs.ls`, `inferSchema` |
| Schema Design | DDL-string schema, `StructType`/`StructField` schema |
| Core Transformations | `select`, `alias`, `filter`, `withColumnRenamed`, `withColumn`, type casting |
| Sorting & Trimming | `sort`, `limit`, `drop`, `dropDuplicates`, `distinct` |
| Set Operations | `union`, `unionByName` |
| String & Date Functions | `upper`, `current_date`, `date_add`, `date_sub`, `datediff`, `date_format` |
| Null Handling | `dropna`, `fillna` (all/any/subset variants) |
| Split & Explode | `split`, array indexing, `explode`, `array_contains` |
| Aggregation | `groupBy` + `agg`, `collect_list`, `pivot` |
| Conditional Logic | `when` / `otherwise` (single and chained) |
| Joins | `inner`, `left`, `right`, `anti` |
| Window Functions | `row_number`, `rank`, `dense_rank`, running/cumulative sums |
| UDFs | Custom Python functions registered as Spark UDFs |
| Data Writing | CSV/Parquet writes, write modes (`append`, `overwrite`, `error`, `ignore`), `saveAsTable` |
| Spark SQL | `createTempView`, querying via `%sql` and `spark.sql()` |

## 📖 Detailed Learnings

### Data Reading & Schema Design
**Focus:** Getting data into Spark reliably, with control over its structure.

- Read JSON and CSV files using the `spark.read` API with options like `inferSchema` and `multiLine`
- Used `dbutils.fs.ls()` to browse the Databricks file system directly from a notebook
- Defined schemas two ways — a **DDL string** and a fully typed **`StructType`/`StructField`** — and understood when each is preferable

```python
my_ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight STRING,
    Item_MRP DOUBLE,
    Outlet_Establishment_Year INT
'''

df = spark.read.format('csv').schema(my_ddl_schema).option('header', True).load('/FileStore/tables/BigMart_Sales.csv')
```
**Takeaway:** Letting Spark infer a schema is fine for exploration, but explicit schemas (DDL or `StructType`) are what production pipelines actually rely on for consistency and speed.

---

### Core & Intermediate Transformations
**Focus:** The everyday operations that make up 80% of real PySpark code.

- Selected and aliased columns, filtered rows with single and multi-condition logic (including `isNull()` and `isin()`)
- Renamed and created columns with `withColumnRenamed` and `withColumn`, including regex-based cleanup with `regexp_replace`
- Sorted with single/multi-column ordering, capped results with `limit`, and dropped columns/duplicates precisely

```python
df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

df = df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg"))
```
**Takeaway:** These transformations are lazy — nothing runs until an action like `.display()` is called, which is the foundation for understanding Spark's execution model.

---

### String, Date, and Null Handling
**Focus:** Cleaning and standardizing real-world messy data.

- Applied string functions (`upper`) and a full suite of date functions (`current_date`, `date_add`, `date_sub`, `datediff`, `date_format`)
- Handled nulls precisely — dropping rows where *all* vs *any* columns are null, or only within a specific subset of columns, and filling nulls with defaults

```python
df = df.withColumn('week_after', date_add('curr_date', 7))
df = df.withColumn('datediff', datediff('week_after', 'curr_date'))
df.dropna(subset=['Outlet_Size']).display()
```
**Takeaway:** Null-handling strategy has to be deliberate — dropping "any" null is very different from dropping only when a specific critical column is missing.

---

### Split, Explode & Aggregation
**Focus:** Turning nested/delimited data into analyzable rows, then summarizing it.

- Split a delimited string column into an array, indexed into it, and used `explode()` to turn array elements into their own rows
- Used `array_contains()` to flag rows containing a specific value
- Performed grouped aggregations (`sum`, `avg`) across single and multiple grouping columns, used `collect_list()` to roll values back up per group, and used `pivot()` to reshape category values into columns

```python
df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))
df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()
```
**Takeaway:** `explode` + `groupBy` + `pivot` together form the toolkit for turning nested, delimited, or categorical data into a clean, analysis-ready shape.

---

### Conditional Logic, Joins & Window Functions
**Focus:** Business rules, multi-table analysis, and per-group analytics — without collapsing rows.

- Built layered `when().otherwise()` logic for multi-condition categorization
- Practiced `inner`, `left`, `right`, and `anti` joins between DataFrames
- Used `row_number()`, `rank()`, and `dense_rank()` over `Window.orderBy()`, and computed running/cumulative sums with `rowsBetween(Window.unboundedPreceding, Window.currentRow)`

```python
df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
  .withColumn('denseRank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

df.withColumn('cumsum', sum('Item_MRP').over(
    Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow)
)).display()
```
**Takeaway:** Window functions are what let Spark compute analytics *per row, per group* — the same skill that separates basic querying from real analytical engineering.

---

### UDFs, Data Writing & Spark SQL
**Focus:** Extending Spark with custom logic, persisting results, and querying with SQL.

- Wrote a plain Python function and registered it as a Spark UDF using `udf()`
- Wrote DataFrames out in CSV and Parquet formats, practicing every write mode (`append`, `overwrite`, `error`, `ignore`) and saving directly as a **managed table** with `saveAsTable`
- Registered a DataFrame as a **temp view** and queried it both via a native `%sql` cell and programmatically with `spark.sql()`

```python
def my_func(x):
    return x * x

my_udf = udf(my_func)
df.withColumn('mynewcol', my_udf('Item_MRP')).display()

df.createTempView('my_view')
df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'Lf'")
```
**Takeaway:** This closed the loop — DataFrame API, custom Python logic, and SQL are three interchangeable ways to work with the same underlying data in Spark.

---

# 📗 Notebook 2 — PySpark Interview Mastery (`PySpark_Interview.dbc`)

This notebook is split into two halves: **hands-on scenario problems** (solved with working code) and **conceptual deep-dives** into how Spark actually works internally — the exact material real interviews probe.

## 🧩 Part A — Real-World Scenario Problems Solved

| # | Scenario | Core Technique |
|---|---|---|
| 1 | Remove duplicates, keep the latest entry by timestamp | `dropDuplicates` + `orderBy` |
| 2 | Merge files with inconsistent schemas | `option("mergeSchema", "true")` |
| 3 | Handle nulls in a streaming category column | `fillna()` |
| 4 | Find the top 5 most active users | `groupBy` + `agg(sum)` + `orderBy` + `limit` |
| 5 | Find each customer's most recent transaction | `dense_rank()` over `Window.partitionBy` |
| 6 | Filter customers inactive for 30+ days | `date_diff` + `current_date()` |
| 7 | Find the most frequent words in feedback text | `explode(split())` + `groupBy(count)` |
| 8 | Cumulative sum of sales per product over time | `sum().over(Window.partitionBy().orderBy())` |
| 9 | Remove duplicates without disturbing original order | `row_number()` + `filter(rowFlag == 1)` |
| 10 | Average session duration per user | `groupBy` + `avg()` |
| 11 | Highest-selling product per month | `dense_rank()` partitioned by month |
| 12 | Ensure ACID-safe concurrent updates on Delta | `DeltaTable.merge()` (upsert) |
| 13 | Enforce schema while reading Parquet | `inferSchema` option |
| 14 | Skip corrupt records while reading CSV | `mode = "DROPMALFORMED"` |
| 15 | Department with the most employees | `groupBy(count)` + `sort` |
| 16 | Classify transactions as High/Low | `when().otherwise()` |
| 17 | Add a processing timestamp column | `current_timestamp()` |
| 18 | Query a DataFrame using SQL | `createOrReplaceTempView` |
| 19 | Share a temp view across notebooks | `createOrReplaceGlobalTempView` |
| 20 | Flatten a nested JSON structure for querying | Dot-notation column access (`struct.field`) |
| 21 | Handle inconsistent JSON schema from an API | `mergeSchema` on read |
| 22 | Optimize Parquet reads by partitioning | `write.partitionBy("column")` |
| 23 | Write Parquet with optimized compression | `option("compression", "snappy")` |
| 24 | Speed up a slow Delta aggregation pipeline | `OPTIMIZE table ZORDER BY (col)` |
| 25 | Group by category and list all product names | `collect_list()` |
| 26 | Group by customer and list unique products purchased | `collect_set()` |
| 27 | Combine first/last name only if email exists | Conditional `concat` with `when` |
| 28 | Count products purchased per customer from a list column | `size()` on array column |
| 29 | Pad employee IDs to a fixed length | `lpad()` |
| 30 | Validate phone numbers starting with "91" | `startswith()` |
| 31 | Average number of courses per student | `groupBy` + `avg(size())` |
| 32 | Use a primary contact number, fallback to secondary | `coalesce()` (column function) |
| 33 | Categorize product codes by their length | `when(length(col) == 5, ...)` |

**Example — Deduplication keeping the most recent record:**
```python
df = df.withColumn('date', col('date').cast(DateType()))
df = df.orderBy('product_id', 'date', ascending=[1, 0]) \
       .dropDuplicates(subset=['product_id'])
```

**Example — Safe upsert into a Delta table:**
```python
from delta.tables import DeltaTable

delta_tbl = DeltaTable.forPath(spark, 'path')
delta_tbl.alias('trg').merge(df.alias('src'), "src.id = trg.id") \
    .whenNotMatchedInsertAll() \
    .whenMatchedUpdateAll() \
    .execute()
```

**Takeaway:** These aren't textbook exercises — they mirror the exact kind of "here's a messy real-world situation, fix it" questions asked in actual data engineering interviews, and each one maps to a pattern reusable in production pipelines.

---

## 🧠 Part B — Spark Internals & Performance Deep-Dives

| Topic | What It Covers |
|---|---|
| Spark Architecture | Driver, SparkContext, Cluster Manager, Worker Nodes, Executors, Tasks, Cache |
| Spark vs Hadoop MapReduce | In-memory vs disk-based processing, latency, use cases |
| RDD vs DataFrame vs Dataset | Schema, type safety, performance, when to use each |
| SparkSession & SparkContext | Entry points to a Spark application and cluster |
| Query Optimization / Catalyst Optimizer | Logical → optimized logical → physical plan, predicate pushdown, column pruning |
| Narrow vs Wide Transformations | Shuffle-free vs shuffle-requiring operations |
| `coalesce()` vs `repartition()` | Reducing vs increasing partitions, shuffle cost |
| `cache()` vs `persist()` | Default vs configurable storage levels |
| Importance of Partitions | Parallelism, fault tolerance, resource utilization |
| `OPTIMIZE` + `ZORDER BY` | Small-file compaction and data-skipping for Delta tables |
| Broadcast Variables | Sharing read-only data efficiently across executors |
| `df.show()` vs `df.collect()` | Driver-safe preview vs pulling all data to the driver |
| Lazy Evaluation | Transformations vs actions, execution planning |
| Delta Lake Advantages | ACID transactions, schema enforcement/evolution, time travel, versioning |
| Out of Memory (OOM) Errors | Causes, Spark's spill behavior, prevention strategies |
| Adaptive Query Execution (AQE) | Dynamic join selection, shuffle partition coalescing, skew join handling |
| Handling Data Skew | Salting, broadcast joins, repartitioning, filtering early |
| Broadcast Join | Avoiding shuffles when joining a large table with a small one |
| Spill | Why memory pressure forces disk writes mid-execution, and how to reduce it |
| Delta Lake Time Travel | Querying and restoring previous table versions via the Delta transaction log |

### Highlighted Deep-Dive: Data Skew & Broadcast Join
Skewed keys (a few values with disproportionately many rows) can bottleneck an entire job on one executor. Practiced multiple mitigation strategies:

```python
# Broadcast join — best when one table is small
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# AQE — lets Spark auto-optimize skewed joins and shuffle partitions at runtime
spark.conf.set("spark.sql.adaptive.enabled", "true")
```
**Takeaway:** Real Spark performance tuning isn't about writing "correct" code — it's about understanding *how* that code executes physically across a cluster, and intervening (broadcast, salting, AQE, repartitioning) when the default execution plan breaks down at scale.

### Highlighted Deep-Dive: Delta Lake Time Travel
```sql
DESCRIBE HISTORY workspace.default.order_man;
SELECT * FROM workspace.default.order_man VERSION AS OF 2;
RESTORE workspace.default.order_man TO VERSION AS OF 4;
```
**Takeaway:** Every Delta table maintains a full transaction log — this turns "we accidentally deleted/corrupted data" from a disaster into a one-line recovery.

---

## 🧠 Skills Demonstrated

- ✅ **PySpark Fundamentals:** Reading, schema design, transformations, joins, window functions, UDFs, writing data
- ✅ **Spark SQL:** Temp views, global temp views, hybrid SQL + DataFrame workflows
- ✅ **Real-World Problem Solving:** 30+ interview-style scenarios covering deduplication, ranking, text analysis, schema drift, and array handling
- ✅ **Delta Lake Engineering:** ACID-safe MERGE/upsert, time travel, `OPTIMIZE` + `ZORDER`
- ✅ **Performance Tuning:** Partitioning, caching strategies, broadcast joins, data skew handling, AQE
- ✅ **Spark Internals Fluency:** Architecture, Catalyst Optimizer, lazy evaluation, narrow vs wide transformations — the exact concepts tested in technical interviews

---

## ▶️ How to Run

### Prerequisites
- Databricks Workspace (Community Edition or full workspace)
- A running cluster with Spark & Delta Lake support

### Steps
1. Import `1_Tutorial.ipynb` and `PySpark_Interview.dbc` into your Databricks workspace
2. Attach each notebook to a cluster
3. Update file paths (`/FileStore/tables/...`) to match your own uploaded datasets
4. Run `1_Tutorial.ipynb` top to bottom to build fundamentals, then work through `PySpark_Interview.dbc` scenario by scenario

---

## 📂 Repository Structure

```
PySpark-Mastery/
├── PySpark-Mastery.ipynb          → PySpark fundamentals: reading, schema, transformations,
│                                joins, window functions, UDFs, writing, Spark SQL
└── PySpark_Interview.dbc     → 30+ real-world interview scenarios + Spark internals
                                 (architecture, Catalyst, partitions, caching, Delta Lake,
                                 AQE, data skew, broadcast joins, time travel)
```

---

