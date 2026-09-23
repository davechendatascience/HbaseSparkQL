# HbaseSparkQL

[![Spark Version](https://img.shields.io/badge/Spark-3.5.x-E25A1C.svg)](https://spark.apache.org/)
[![HBase Version](https://img.shields.io/badge/HBase-2.5.x%20%2F%202.6.x-C02A3E.svg)](https://hbase.apache.org/)
[![Scala Version](https://img.shields.io/badge/Scala-2.12.18-DC322F.svg)](https://www.scala-lang.org/)
[![sbt](https://img.shields.io/badge/sbt-1.9.9-005A9C.svg)](https://www.scala-sbt.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**HbaseSparkQL** is a high-performance Apache Spark & Apache HBase connector that empowers the Spark execution engine to execute distributed, partition-aware native queries against HBase using an intuitive, SQL-like query language (**DistHQL**).

Unlike standard batch scans that suffer from severe partition serialization bottlenecks, HbaseSparkQL leverages `HBaseContext` connection pooling across executors to execute distributed point `GET`s, bounded `SCAN`s, atomic `PUT`s, HFile bulk-loads, and `DELETE`s directly within Spark DataFrames and RDDs.

---

## Key Features

* **DistHQL Query Engine**: Expressive, natural query syntax to orchestrate distributed HBase operations (`GET`, `SCAN`, `PUT`, `APPEND`, `DELETE`) without writing repetitive client boilerplate.
* **Distributed Batching**: Uses executor-level persistent connection pooling and configurable batch sizes to maximize throughput while preventing RegionServer overload.
* **HFile Bulk-Loading**: Optional high-throughput `PUT ... USE_HFILE` mode that bypasses the Write-Ahead Log (WAL) to directly write HFiles into HDFS and trigger bulk loads.
* **Vector Analytics & Aggregation**: Integrated Spark ML linear algebra support (`org.apache.spark.ml.linalg`), including vector aggregators (`vector_sum`, `vector_mean`), element-wise arithmetic, and nested JSON document feature extraction.
* **Modern Big Data Stack**: Modernized for **Apache Spark 3.5+**, **Apache HBase 2.5+ / 2.6+**, **Scala 2.12**, and **Java 8 / 11 / 17**.

---

## Ecosystem & Compatibility

| Component | Target Version | Supported Versions |
| :--- | :--- | :--- |
| **Apache Spark** | `3.5.1` | Spark 3.4.x – 3.5.x |
| **Apache HBase** | `2.5.8` | HBase 2.4.x, 2.5.x, 2.6.x (HBase 3.0 ready) |
| **Apache Hadoop** | `3.3.6` | Hadoop 3.2.x – 3.3.x |
| **HBase-Spark Connector** | `1.0.0` | `org.apache.hbase.connectors.spark:hbase-spark3` |
| **Scala** | `2.12.18` | 2.12.x / 2.13.x |
| **JDK** | JDK 11 / 17 | JDK 8, 11, 17 |

*For details on upgrading from legacy Spark 1.6 / HBase 2.0-alpha, read our [Modernization & Migration Guide](docs/MIGRATION_GUIDE.md).*

---

## Architecture & DistHQL Query Syntax

DistHQL allows you to interact with HBase tables directly from Spark using clean, declarative statements:

```
[ACTION] [TARGET] [SPECIFICATION] [FILTERS]
```

### 1. Distributed Point Lookups (`GET`)

Query target rows in HBase using a Spark DataFrame containing row keys:

```sql
-- Point GET with keys from a DataFrame
GET FROM users WITH keys_df WITH_KEYS user_id

-- Point GET targeting a specific column family
GET FROM users WITH keys_df WITH_KEYS user_id USE_FAMILY profile

-- Point GET for specific qualifiers
GET FROM users WITH keys_df WITH_KEYS user_id USE_FAMILY profile WITH_QUALIFIERS email

-- Point GET with qualifier range filters
GET FROM users WITH keys_df WITH_KEYS user_id WITH_QUALIFIER_FILTERS col_min col_max

-- Point GET with value filters
GET FROM users WITH keys_df WITH_KEYS user_id WITH_VALUE_FILTERS val_min val_max
```

### 2. Distributed Range Scans (`SCAN`)

Perform distributed scans across table regions with server-side filters:

```sql
-- Scan rows within a column family
SCAN FROM events USE_FAMILY telemetry

-- Scan with qualifier range filter (inclusive)
SCAN FROM events USE_FAMILY telemetry USE_QUALIFIER_FILTER cpu_0 cpu_9

-- Scan with value bounds
SCAN FROM metrics USE_FAMILY stats USE_VALUE_FILTER 100 500

-- Scan bounded by epoch timestamp
SCAN FROM logs USE_FAMILY info USE_TIMESTAMP_FILTER 1700000000000 1700003600000
```

### 3. Distributed Ingest & Bulk Loads (`PUT`)

Write DataFrame records formatted as `rowKey :: columnFamily :: columnQualifier :: value`:

```sql
-- Standard distributed batch PUT
PUT INTO events WITH_RECORDS records_view

-- Ultra-fast bulk load via direct HFile creation (bypasses WAL)
PUT INTO events WITH_RECORDS records_view USE_HFILE
```

### 4. Distributed Merging (`APPEND`)

Atomically merge incoming record updates with existing cell values:

```sql
APPEND ONTO user_sessions WITH_RECORDS session_updates_view
```

### 5. Distributed Deletions (`DELETE`)

Delete rows or specific column families/qualifiers at scale:

```sql
-- Delete entire rows specified in DataFrame
DELETE FROM sessions WITH expired_keys_view WITH_KEYS session_id

-- Delete specific column family
DELETE FROM sessions WITH expired_keys_view WITH_KEYS session_id USE_FAMILY cache

-- Delete specific column qualifier
DELETE FROM sessions WITH expired_keys_view WITH_KEYS session_id USE_FAMILY cache WITH_QUALIFIERS token
```

---

## Quickstart

### 1. Initialize SparkSession & HBaseContext

```scala
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.spark.HBaseContext
import HbaseSparkQL.HbaseHelper._

val spark = SparkSession.builder()
  .appName("HbaseSparkQL-Application")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

// Create the partition-aware HBase context
val hbaseContext = create_hbase_context(spark)
```

### 2. Execute a Distributed Query

```scala
// 1. Prepare a DataFrame containing keys to query
val keysDf = Seq("user_1001", "user_1002", "user_1003").toDF("userId")
keysDf.createOrReplaceTempView("keys_to_fetch")

// 2. Execute DistHQL
val resultDf = DistHQL(
  spark, 
  hbaseContext, 
  "GET FROM users WITH keys_to_fetch WITH_KEYS userId USE_FAMILY profile"
)

resultDf.show(truncate = false)
```

### 3. Vector Arithmetic & Analytics

```scala
import HbaseSparkQL.FeatureHelper._
import org.apache.spark.ml.linalg.Vectors

val v1 = Vectors.dense(1.0, 2.0, 3.0)
val v2 = Vectors.dense(4.0, 5.0, 6.0)

// Element-wise vector addition & subtraction
val sum = vector_addition(v1, v2)       // [5.0, 7.0, 9.0]
val diff = vector_subtraction(v2, v1)   // [3.0, 3.0, 3.0]
```

---

## Configuration

HbaseSparkQL loads configuration through Typesafe Config from `src/main/resources/config.json`:

```json
{
    "sparkMaster": "local[*]",
    "hadoop": "hdfs://localhost:9000",
    "HbaseHelper": {
        "separator": " :: ",
        "hbase_site_path": "conf/hbase-site.xml",
        "hadoop_core_site_path": "conf/core-site.xml",
        "hadoop_metrics2_hbase_properties": "conf/hadoop-metrics2-hbase.properties",
        "hadoop_metrics2_properties": "conf/hadoop-metrics2.properties"
    }
}
```

* **`separator`**: Delimiter string used to format structured row, family, qualifier, and value strings (default: `" :: "`).
* **`hbase_site_path`**: Path to your cluster's `hbase-site.xml` file.
* **`hadoop_core_site_path`**: Path to your cluster's `core-site.xml` file.

---

## Building, Testing & Packaging

### Prerequisites
* **Java JDK**: 11 or 17 (or JDK 8)
* **sbt**: 1.9.0 or higher

### Running Unit Tests
```bash
sbt test
```

### Building the Shaded Fat JAR
```bash
sbt clean compile assembly
```
The packaged fat JAR will be generated at:
`target/scala-2.12/hbase-spark-query-language-assembly-0.1.0.jar`

### Submitting to a Spark Cluster
```bash
spark-submit \
  --class com.example.YourApplication \
  --master yarn \
  --deploy-mode cluster \
  --jars target/scala-2.12/hbase-spark-query-language-assembly-0.1.0.jar \
  your-application.jar
```

---

## License

This project is licensed under the Apache 2.0 License.
