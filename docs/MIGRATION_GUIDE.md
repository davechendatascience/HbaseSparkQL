# HbaseSparkQL Modernization & Migration Guide

This guide provides a comprehensive, production-grade roadmap for migrating **HbaseSparkQL** from its legacy 2016 stack (**Spark 1.6**, **HBase 2.0.0-alpha2**, **Scala 2.10**, and **sbt 0.13**) to the modern Big Data ecosystem (**Spark 3.5.x**, **HBase 2.5.x / 2.6.x**, **Scala 2.12 / 2.13**, and **sbt 1.9+**).

---

## Table of Contents

1. [Executive Summary & Motivation](#1-executive-summary--motivation)
2. [Target Version & Compatibility Matrix](#2-target-version--compatibility-matrix)
3. [Build System & Dependency Modernization (`build.sbt`)](#3-build-system--dependency-modernization-buildsbt)
4. [File-by-File Codebase Migration & Deprecations](#4-file-by-file-codebase-migration--deprecations)
   - [4.1 `HbaseHelper.scala`](#41-hbasehelperscala)
   - [4.2 `SqlHelper.scala`](#42-sqlhelperscala)
   - [4.3 `FeatureHelper.scala`](#43-featurehelperscala)
   - [4.4 `Configs.scala`](#44-configsscala)
   - [4.5 `config.json`](#45-configjson)
5. [DistHQL Mini-Language: Evolution & Modern DSL](#5-disthql-mini-language-evolution--modern-dsl)
6. [Testing, Packaging & Verification Strategy](#6-testing-packaging--verification-strategy)
7. [Step-by-Step Developer Migration Checklist](#7-step-by-step-developer-migration-checklist)

---

## 1. Executive Summary & Motivation

The original implementation of `HbaseSparkQL` was authored in 2016–2017. While the core premise—enabling Spark to execute distributed, high-throughput queries, bulk gets, bulk puts, and bulk scans against Apache HBase using an expressive SQL-like syntax—remains highly valuable, the foundational libraries have undergone fundamental generational shifts:

* **Spark 1.6 $\to$ Spark 3.5+**: The deprecated `SQLContext` and `DataFrame` RDD-bridging methods (`registerTempTable`, `unionAll`) have been superseded by `SparkSession`, Dataset-based optimizations (Catalyst / Tungsten), and modern extension APIs. The legacy `UserDefinedAggregateFunction` (UDAF) API was removed in Spark 3.2+ in favor of typed `Aggregator`s.
* **HBase 2.0.0-alpha2 $\to$ HBase 2.5.x / 2.6.x / 3.0**: Deprecated table management classes (`HTableDescriptor`, `HColumnDescriptor`) have been completely superseded by descriptor builders (`TableDescriptorBuilder`, `ColumnFamilyDescriptorBuilder`). Legacy filter enums (`CompareFilter.CompareOp`) have transitioned to `CompareOperator`. Bulk loading has evolved from `LoadIncrementalHFiles` to `BulkLoadHFiles`.
* **Scala 2.10 $\to$ Scala 2.12 / 2.13**: Scala 2.10 reached End-of-Life in 2017. Spark 3.5 standardizes on Scala 2.12 (with Scala 2.13 support) and requires Java 8, 11, or 17.
* **sbt 0.13 $\to$ sbt 1.9+**: Deprecated sbt 0.13 operators (`<<=`) are incompatible with modern sbt. Insecure HTTP Maven repositories are rejected by modern build engines.

---

## 2. Target Version & Compatibility Matrix

| Component | Legacy Version | Modern Target Version | Rationale & Notes |
| :--- | :--- | :--- | :--- |
| **Apache Spark** | `1.6.0` | **`3.5.1`** (or `3.5.x`) | Mainstream production release; full Catalyst support, modern Catalyst UDAF and Catalyst rules. |
| **Apache HBase** | `2.0.0-alpha2` | **`2.5.8`** / **`2.6.0`** | Mature HBase 2.x line; stable client protocol, compatible with Hadoop 3.3+. |
| **Apache Hadoop** | `2.7.1` | **`3.3.6`** | Spark 3.5 defaults to Hadoop 3.3.6 client bindings. |
| **HBase-Spark Connector** | `hbase-spark` 2.0.0-alpha2 | **`hbase-spark3` 1.0.0+** (`org.apache.hbase.connectors.spark`) | Maintained under the Apache HBase Connectors subproject for Spark 3. |
| **Scala** | `2.10.4` | **`2.12.18`** (optionally `2.13.12`) | Standard Spark 3.x runtime; standard Java 8/11/17 bytecode. |
| **sbt** | `0.13.15` | **`1.9.9`** | Modern compiler bridge, native parallel task execution, dependency caching. |
| **sbt-assembly** | `0.14.3` | **`2.1.5`** | Modern syntax for shaded fat JAR creation. |
| **Java JDK** | `1.8` only (enforced) | **JDK 8, 11, or 17** | Spark 3.5 officially supports JDK 8/11/17; remove restrictive runtime assertion. |
| **JSON Library** | `net.liftweb:lift-json:2.6+` | **`org.json4s:json4s-jackson:3.7.0-M11`** | Lift JSON 2.6 does not exist for modern Scala; JSON4S is already bundled with Spark. |
| **Date/Time** | `joda-time:joda-time:2.9.4` | **`java.time` (Java 8+)** | Native standard library replaces external Joda-Time dependency. |
| **Testing** | `scalatest:2.2.6` | **`org.scalatest:scalatest:3.2.18`** | Standard modern unit test framework. |

---

## 3. Build System & Dependency Modernization (`build.sbt`)

### 3.1 `project/build.properties`

Update the sbt version:
```properties
sbt.version=1.9.9
```

### 3.2 `project/assembly.sbt`

Upgrade `sbt-assembly` plugin:
```scala
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "2.1.5")
```

### 3.3 Modern `build.sbt` Replacement

Key updates:
1. Replace `<<=` syntax with modern `:=`.
2. Convert all Maven repositories to `https://`.
3. Align dependencies with Spark 3.5 and HBase 2.5.
4. Replace `net.liftweb` with `org.json4s`.
5. Remove JDK 1.8 hard assertion to support modern JDKs (JDK 8/11/17).

```scala
name := "hbase-spark-query-language"

organization := "com.github.davechendatascience"

version := "0.1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.1"
val hbaseVersion = "2.5.8"
val hbaseConnectorsVersion = "1.0.0"
val hadoopVersion = "3.3.6"
val json4sVersion = "3.7.0-M11"
val typesafeConfigVersion = "1.4.3"
val scalaTestVersion = "3.2.18"

libraryDependencies ++= Seq(
  // Spark 3.5.x
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Apache HBase Client & Common
  "org.apache.hbase" % "hbase-client" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hbaseVersion % "provided",
  "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion % "provided",

  // Apache HBase Connectors for Spark 3
  // Distributed via Cloudera or Apache repo
  "org.apache.hbase.connectors.spark" % "hbase-spark3" % hbaseConnectorsVersion % "provided",

  // Configuration & Utilities
  "com.typesafe" % "config" % typesafeConfigVersion,
  "org.json4s" %% "json4s-jackson" % json4sVersion,

  // Testing
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
)

resolvers ++= Seq(
  "Apache Releases" at "https://repository.apache.org/content/repositories/releases/",
  "Apache Snapshots" at "https://repository.apache.org/content/repositories/snapshots/",
  "Cloudera Repositories" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  Resolver.mavenCentral,
  Resolver.sonatypeRepo("public")
)

// Modern sbt-assembly 2.x Merge Strategy
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) =>
    xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _      => MergeStrategy.concat
      case _                    => MergeStrategy.discard
    }
  case PathList("javax", "servlet", _ @ _*) => MergeStrategy.first
  case PathList("org", "apache", _ @ _*)    => MergeStrategy.first
  case PathList("org", "slf4j", _ @ _*)     => MergeStrategy.first
  case "reference.conf"                     => MergeStrategy.concat
  case "application.conf"                   => MergeStrategy.concat
  case _                                    => MergeStrategy.first
}
```

---

## 4. File-by-File Codebase Migration & Deprecations

### 4.1 `HbaseHelper.scala`

#### A. Entry Points: From `SQLContext` to `SparkSession`
* **Legacy:** Methods accept `(sc: SparkContext, sqlc: SQLContext, ...)`.
* **Modern:** Spark 3 unifies contexts into `SparkSession`.

```scala
// BEFORE (Spark 1.6)
def DistHQL(sc: SparkContext, sqlc: SQLContext, hbaseContext: HBaseContext, HQL_string: String): DataFrame = {
    import sqlc.implicits._
    val empty_string_df = sqlc.createDataFrame(sc.emptyRDD[Row], pb_schema)
    ...
}

// AFTER (Spark 3.5)
def DistHQL(spark: SparkSession, hbaseContext: HBaseContext, HQL_string: String): DataFrame = {
    import spark.implicits._
    val empty_string_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], pb_schema)
    ...
}
```

#### B. Temporary View Registration
* `DataFrame.registerTempTable(...)` was removed in Spark 2.x+.
* **Replacement:** `DataFrame.createOrReplaceTempView(...)`.

```scala
// BEFORE
trans_result.registerTempTable("trans_result_" + tableName)

// AFTER
trans_result.createOrReplaceTempView("trans_result_" + tableName)
```

#### C. DataFrame Concatenation
* `DataFrame.unionAll(...)` was deprecated in Spark 2.0 and replaced with `DataFrame.union(...)`.

```scala
// BEFORE
val union_records_df = records_df.unionAll(old_records_df)

// AFTER
val union_records_df = records_df.union(old_records_df)
```

#### D. Table & Column Descriptors
* `HTableDescriptor` and `HColumnDescriptor` are deprecated in HBase 2.x and removed in HBase 3.0.
* **Replacement:** `TableDescriptorBuilder` and `ColumnFamilyDescriptorBuilder`.

```scala
// BEFORE (HBase 1.x / early 2.0-alpha)
def create_table(tableName: String, families: Seq[String]) = {
    ...
    val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
    for (fam <- families) {
        tableDescriptor.addFamily(new HColumnDescriptor(fam))
    }
    admin.createTable(tableDescriptor)
}

// AFTER (HBase 2.5+ / 3.0 compatible)
def create_table(tableName: String, families: Seq[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.addResource(new Path(hadoop_core_site_path))
    conf.addResource(new Path(hbase_site_path))
    
    val conn = ConnectionFactory.createConnection(conf)
    try {
        val admin = conn.getAdmin
        try {
            val tableDescBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
            for (fam <- families) {
                tableDescBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam))
            }
            admin.createTable(tableDescBuilder.build())
            println(s"Table created: $tableName")
        } finally {
            admin.close()
        }
    } finally {
        conn.close()
    }
}
```

#### E. Filter Operator Enums
* `CompareFilter.CompareOp` is deprecated in HBase 2.0.
* **Replacement:** `org.apache.hadoop.hbase.CompareOperator`.

```scala
// BEFORE
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
new FamilyFilter(CompareOp.EQUAL, new RegexStringComparator("^" + family_str + "$"))
new ValueFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(min_str)))

// AFTER
import org.apache.hadoop.hbase.CompareOperator
new FamilyFilter(CompareOperator.EQUAL, new RegexStringComparator("^" + family_str + "$"))
new ValueFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(min_str)))
```

#### F. HFile Bulk Loading
* `LoadIncrementalHFiles` was moved to `org.apache.hadoop.hbase.tool.LoadIncrementalHFiles` and wrapped by `BulkLoadHFiles`.

```scala
// BEFORE
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
val load = new LoadIncrementalHFiles(conf)
load.doBulkLoad(path, admin, table, regionLocator)

// AFTER (HBase 2.5+)
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
val bulkLoader = BulkLoadHFiles.create(conf)
bulkLoader.bulkLoad(TableName.valueOf(hbaseTableName), path)
```

---

### 4.2 `SqlHelper.scala`

#### A. UDAF Modernization: From `UserDefinedAggregateFunction` to `Aggregator`
Spark 3.0 deprecated `UserDefinedAggregateFunction` and Spark 3.2 removed it. Custom aggregations now extend `org.apache.spark.sql.expressions.Aggregator[IN, BUF, OUT]` or utilize Spark's built-in array/set functions.

##### Example: Replacing `AggregateToSet`
In Spark 1.6, a custom UDAF was written to aggregate elements into a distinct list:
```scala
// BEFORE (Custom UDAF in Spark 1.6)
class AggregateToSet(aggTypeString: String) extends UserDefinedAggregateFunction { ... }
```

In Spark 3.5, this is built directly into Spark SQL using `collect_set`:
```scala
// AFTER (Spark 3.5 built-in SQL)
import org.apache.spark.sql.functions.collect_set

df.groupBy("key").agg(collect_set($"col").alias("set_values"))
```

##### Example: Modern `vector_sum` & `vector_mean` using `Aggregator`
For vector math, migrate from `org.apache.spark.mllib.linalg.Vector` to `org.apache.spark.ml.linalg.Vector` and use typed `Aggregator`:

```scala
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf

case class VectorSumBuffer(var sum: Array[Double], var initialized: Boolean)

object VectorSumAggregator extends Aggregator[Vector, VectorSumBuffer, Vector] {
    override def zero: VectorSumBuffer = VectorSumBuffer(Array.emptyDoubleArray, initialized = false)

    override def reduce(buffer: VectorSumBuffer, input: Vector): VectorSumBuffer = {
        if (input != null) {
            if (!buffer.initialized) {
                buffer.sum = input.toArray.clone()
                buffer.initialized = true
            } else {
                val inputArr = input.toArray
                var i = 0
                while (i < buffer.sum.length && i < inputArr.length) {
                    buffer.sum(i) += inputArr(i)
                    i += 1
                }
            }
        }
        buffer
    }

    override def merge(b1: VectorSumBuffer, b2: VectorSumBuffer): VectorSumBuffer = {
        if (!b1.initialized) b2
        else if (!b2.initialized) b1
        else {
            var i = 0
            while (i < b1.sum.length && i < b2.sum.length) {
                b1.sum(i) += b2.sum(i)
                i += 1
            }
            b1
        }
    }

    override def finish(reduction: VectorSumBuffer): Vector = {
        if (!reduction.initialized) Vectors.dense(Array.emptyDoubleArray)
        else Vectors.dense(reduction.sum)
    }

    override def bufferEncoder: Encoder[VectorSumBuffer] = Encoders.product[VectorSumBuffer]
    override def outputEncoder: Encoder[Vector] = org.apache.spark.sql.catalyst.encoders.ExpressionEncoder[Vector]()
}

// Registration in Spark 3.5:
val vectorSumUdaf = udaf(VectorSumAggregator)
spark.udf.register("vector_sum", vectorSumUdaf)
```

*(Alternatively, Spark ML's built-in `org.apache.spark.ml.stat.Summarizer.sum($"vectorCol")` and `Summarizer.mean($"vectorCol")` can be used directly without custom code).*

#### B. Modern `dfZipWithIndex`
In Spark 3.5, DataFrame zipWithIndex can use the Window function `row_number()` or `monotonically_increasing_id()` to avoid dropping down to RDD:

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

def dfZipWithIndex(
    df: DataFrame,
    offset: Long = 1L,
    colName: String = "id"
): DataFrame = {
    val windowSpec = Window.orderBy(monotonically_increasing_id())
    df.withColumn(colName, row_number().over(windowSpec) + (offset - 1L))
}
```

---

### 4.3 `FeatureHelper.scala`

#### A. MLlib $\to$ ML Linear Algebra
Replace `org.apache.spark.mllib.linalg._` with `org.apache.spark.ml.linalg._`.

```scala
// BEFORE
import org.apache.spark.mllib.linalg.{Vector, Vectors, DenseVector, SparseVector}

// AFTER
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector, SparseVector}
```

#### B. Removing `net.liftweb.json` in Favor of `org.json4s`
Lift JSON is no longer maintained for modern Scala. Replace with JSON4S Jackson:

```scala
// BEFORE (Lift JSON)
import net.liftweb.json._
val f_val = doc \ f

// AFTER (JSON4S Jackson)
import org.json4s._
import org.json4s.jackson.JsonMethods._

def get_feature_val(doc: JValue, f_name: String): JValue = {
    val f_list = f_name.split('.')
    var f_val = doc
    for (f <- f_list) {
        f_val = f_val \ f
        if (f_val == JNothing || f_val == JNull) {
            return JString("NA")
        }
    }
    f_val
}
```

---

### 4.4 `Configs.scala`

#### A. Scala Collection Converters
Replace `scala.collection.JavaConverters` with `scala.jdk.CollectionConverters` (or cross-compatible syntax):

```scala
// BEFORE (Scala 2.10 / 2.11)
import scala.collection.JavaConverters._

def getList(path: String): List[String] = {
    conf.getStringList(path).asScala.toList
}

// AFTER (Scala 2.12 / 2.13)
import scala.collection.JavaConverters._ // In 2.12
// In Scala 2.13+: import scala.jdk.CollectionConverters._

def getList(path: String): List[String] = {
    conf.getStringList(path).asScala.toList
}
```

---

### 4.5 `config.json`

Eliminate hardcoded cluster URIs (`dmp1:7077`, `hdfs://dmp1:9000`) in favor of environment-driven or fallback configurations:

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

---

## 5. DistHQL Mini-Language: Evolution & Modern DSL

The core interface of `HbaseSparkQL` is its English-like query syntax:
```sql
GET FROM hbase_table WITH query_spec_table WITH_KEYS keys1 USE_FAMILY family1
DELETE FROM hbase_table WITH query_spec_table WITH_KEYS keys1
SCAN FROM hbase_table USE_FAMILY family1 USE_QUALIFIER_FILTER minQ maxQ
PUT INTO hbase_table WITH_RECORDS records1
APPEND ONTO hbase_table WITH_RECORDS records1
```

### Modern Enhancements
To maximize developer productivity in Spark 3.5, `HbaseSparkQL` should provide **both**:
1. **Backward-Compatible Query String Parser** (`DistHQL(spark, hbaseContext, query)`): Retaining exact string syntax.
2. **Type-Safe Fluent DataFrame Extensions**:

```scala
package object HbaseSparkQL {
    implicit class HbaseDataFrameOps(df: DataFrame) {
        def hbaseGet(hbaseContext: HBaseContext, tableName: String, keyCol: String, family: Option[String] = None): DataFrame = {
            // High-level bulkGet delegation
        }
        
        def hbasePut(hbaseContext: HBaseContext, tableName: String, keyCol: String, familyCol: String, qualifierCol: String, valueCol: String): Unit = {
            // High-level bulkPut delegation
        }

        def hbaseDelete(hbaseContext: HBaseContext, tableName: String, keyCol: String): Unit = {
            // High-level bulkDelete delegation
        }
    }
}
```

Usage in Spark 3.5 application:
```scala
import HbaseSparkQL._

val queryDf = spark.read.table("keys_to_fetch")
val resultsDf = queryDf.hbaseGet(hbaseContext, tableName = "users", keyCol = "user_id", family = Some("profile"))
```

---

## 6. Testing, Packaging & Verification Strategy

### 6.1 Modern ScalaTest Suite
Add unit tests under `src/test/scala/HbaseSparkQL/`:
* Use ScalaTest 3.2: `org.scalatest.flatspec.AnyFlatSpec` and `org.scalatest.matchers.should.Matchers`.
* Use `SparkSession.builder().master("local[2]").appName("HbaseSparkQL-Test").getOrCreate()` for headless CI testing.
* Use `HBaseTestingUtility` / `MiniHBaseCluster` for integration tests.

### 6.2 Assembly & Packaging
Generate shaded JAR:
```bash
sbt clean compile test assembly
```

The resulting artifact can be submitted directly with `spark-submit`:
```bash
spark-submit \
  --class com.example.YourMain \
  --master yarn \
  --deploy-mode cluster \
  --jars hbase-spark-query-language-assembly-0.1.0.jar \
  your-application.jar
```

---

## 7. Step-by-Step Developer Migration Checklist

- [ ] **Phase 1: Build Environment**
  - [ ] Install JDK 11 or JDK 17 and sbt 1.9+.
  - [ ] Update `project/build.properties` to `sbt.version=1.9.9`.
  - [ ] Update `project/assembly.sbt` to `sbt-assembly 2.1.5`.
  - [ ] Rewrite `build.sbt` using modern settings and HTTPS repositories.
- [ ] **Phase 2: Core Library Updates**
  - [ ] Migrate `Configs.scala` to use Scala 2.12/2.13 collection conversions.
  - [ ] Replace `net.liftweb.json` with `org.json4s` in `FeatureHelper.scala`.
  - [ ] Upgrade MLlib vector math to `org.apache.spark.ml.linalg` in `FeatureHelper.scala`.
- [ ] **Phase 3: SQL & UDAF Modernization**
  - [ ] Rewrite `vector_sum`, `vector_mean` as Spark 3 `Aggregator`s or leverage `Summarizer` in `SqlHelper.scala`.
  - [ ] Replace custom `AggregateToSet` with built-in `collect_set` in `SqlHelper.scala`.
- [ ] **Phase 4: HBase Client Modernization**
  - [ ] Update `create_table` in `HbaseHelper.scala` to `TableDescriptorBuilder` and `ColumnFamilyDescriptorBuilder`.
  - [ ] Replace `CompareOp` with `CompareOperator`.
  - [ ] Replace `registerTempTable` with `createOrReplaceTempView`.
  - [ ] Replace `unionAll` with `union`.
  - [ ] Update bulk load to `BulkLoadHFiles`.
  - [ ] Overload `DistHQL` to accept `SparkSession`.
- [ ] **Phase 5: Quality Assurance & Release**
  - [ ] Implement unit test suite under `src/test/scala/`.
  - [ ] Verify `sbt compile` and `sbt assembly`.
  - [ ] Validate on local Spark 3.5 / HBase 2.5 cluster.
