package HbaseSparkQL

import java.io.IOException
import scala.util.control.Breaks._
import scala.util.Try

// json4s
import org.json4s._
import org.json4s.jackson.JsonMethods._

// native modules
import HbaseSparkQL.FeatureHelper.{vector_addition}
import HbaseSparkQL.SqlHelper.{registerAggregateToStringSetUdaf}
import HbaseSparkQL.utilities.Configs

// Java 8+ / Joda datetime
import java.time.Instant

// HBase & Hadoop imports
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.{Cell, CellUtil, KeyValue}
import org.apache.hadoop.hbase.client.{
    Connection,
    ConnectionFactory,
    Delete,
    Get,
    Put,
    Result,
    Scan,
    Table,
    TableDescriptorBuilder,
    ColumnFamilyDescriptorBuilder,
    Admin
}
import org.apache.hadoop.hbase.filter.{
    ColumnRangeFilter,
    FamilyFilter,
    FilterList,
    ValueFilter,
    BinaryComparator,
    RegexStringComparator
}
import org.apache.hadoop.hbase.CompareOperator
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.tool.BulkLoadHFiles
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier
import org.apache.hadoop.fs.{Path, FileSystem}

// Spark core & SQL
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame, Row, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{Vectors, Vector}

package object HbaseHelper extends Serializable {
    
    // constants
    val default_batch_size = 1000
    val hadoop_core_site_path = Configs.getOrElse("HbaseHelper.hadoop_core_site_path", "conf/core-site.xml")
    val hbase_site_path = Configs.getOrElse("HbaseHelper.hbase_site_path", "conf/hbase-site.xml")
    val hadoop_metrics2_hbase_properties = Configs.getOrElse("HbaseHelper.hadoop_metrics2_hbase_properties", "conf/hadoop-metrics2-hbase.properties")
    val hadoop_metrics2_properties = Configs.getOrElse("HbaseHelper.hadoop_metrics2_properties", "conf/hadoop-metrics2.properties")
    val universal_sepStr = Configs.getOrElse("HbaseHelper.separator", " :: ")

    val HQL_reserved_words = Map(
        "get" -> Array(
            "from",
            "with",
            "with_keys",
            "use_family",
            "with_qualifiers",
            "with_qualifier_filters",
            "with_value_filters"
        ),
        "delete" -> Array(
            "from",
            "with",
            "with_keys",
            "use_family",
            "with_qualifiers"
        ),
        "scan" -> Array(
            "from",
            "use_family",
            "use_qualifier_filter",
            "use_value_filter",
            "use_timestamp_filter"
        ),
        "put" -> Array(
            "into",
            "with_records"
        ),
        "append" -> Array(
            "onto",
            "with_records"
        )
    )
    val referHbaseTable_keywords = Seq("into", "onto", "from")

    // case classes
    case class HBaseHelperException(message: String = "") extends Exception(message)
    case class output_string(output: String)
    case class keyed_output(id: String, output: String)

    def getHBaseConfiguration(): org.apache.hadoop.conf.Configuration = {
        val conf = HBaseConfiguration.create()
        conf.addResource(new Path(hadoop_core_site_path))
        conf.addResource(new Path(hbase_site_path))
        conf
    }

    // create an hbase context for the query engine.
    @throws(classOf[Exception])
    def create_hbase_context(sc: SparkContext): HBaseContext = {
        val conf = getHBaseConfiguration()
        new HBaseContext(sc, conf)
    }

    @throws(classOf[Exception])
    def create_hbase_context(spark: SparkSession): HBaseContext = {
        create_hbase_context(spark.sparkContext)
    }
    
    // create a table in hbase (modern HBase 2.5+ / 3.0 compatible)
    def create_table(tableName: String, families: Seq[String]): Unit = {
        val conf = getHBaseConfiguration()
        val conn = ConnectionFactory.createConnection(conf)
        try {
            val admin = conn.getAdmin
            try {
                val tableDescBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                for (fam <- families) {
                    tableDescBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(fam))
                }
                admin.createTable(tableDescBuilder.build())
                println("Table created: " + tableName)
            } finally {
                admin.close()
            }
        } finally {
            conn.close()
        }
    }
    
    // delete a table in hbase
    def delete_table(tableName: String): Unit = {
        val conf = getHBaseConfiguration()
        val conn = ConnectionFactory.createConnection(conf)
        try {
            val admin = conn.getAdmin
            try {
                val tn = TableName.valueOf(tableName)
                if (admin.tableExists(tn)) {
                    if (admin.isTableEnabled(tn)) {
                        admin.disableTable(tn)
                    }
                    admin.deleteTable(tn)
                    println("Table deleted: " + tableName)
                }
            } finally {
                admin.close()
            }
        } finally {
            conn.close()
        }
    }
    
    def valueOfCell(cell: Cell): String = {
        val ByteVal = CellUtil.cloneValue(cell)
        if (ByteVal.length > 0 && ByteVal(0) == 0) {
            BigInt(ByteVal).toString
        } else {
            Bytes.toString(ByteVal)
        }
    }

    // return formatted string result from a single row result
    def formatted_string_result(result: Result, sepChar: String): String = {
        val sep = universal_sepStr
        var string_result = ""
        try {
            val it = result.listCells().iterator()
            val b = new StringBuilder
            while (it.hasNext) {
                val cell = it.next()
                val k = Bytes.toString(CellUtil.cloneRow(cell))
                val c = Bytes.toString(CellUtil.cloneFamily(cell))
                val q = Bytes.toString(CellUtil.cloneQualifier(cell))
                val v = valueOfCell(cell)
                val t = cell.getTimestamp
                b.append(k + sep + c + sep + q + sep + v + sep + t + sepChar)
            }
            string_result = b.toString
            if (string_result.nonEmpty) {
                string_result = string_result.substring(0, string_result.length - 1)
            }
        } catch {
            case e @ (_ : java.lang.NullPointerException |
                      _ : java.lang.ArrayIndexOutOfBoundsException) =>
                string_result = "NA"
        }
        string_result
    }
    
    @throws(classOf[Exception])
    def configuredBulkScanRowKeys(
        hbaseContext: HBaseContext,
        tableName: String,
        cache_size: Int,
        family: String,
        minQualifier: String,
        maxQualifier: String
    ): RDD[String] = {
        val filterList = new FilterList()
        filterList.addFilter(new FamilyFilter(CompareOperator.EQUAL, new RegexStringComparator("^" + family + "$")))
        filterList.addFilter(new ColumnRangeFilter(Bytes.toBytes(minQualifier), true, Bytes.toBytes(maxQualifier), true))
        val scan = new Scan()
        scan.setCaching(cache_size)
        scan.setFilter(filterList)
        val getRdd = hbaseContext.hbaseRDD(TableName.valueOf(tableName), scan)
        getRdd.map(v => Bytes.toString(v._1.get()))
    }
    
    // SparkSession-based mergeRows
    def mergeRows(spark: SparkSession, hbaseContext: HBaseContext, rowPairsDF: DataFrame, tableName: String): Unit = {
        import spark.implicits._
        val sepStr = universal_sepStr
        
        val rowPairsDF_filt = rowPairsDF.filter("row1 != row2")
        
        if (!rowPairsDF_filt.rdd.isEmpty()) {
            rowPairsDF_filt.select("row1").createOrReplaceTempView("row1_" + tableName)
            rowPairsDF_filt.select("row2").createOrReplaceTempView("row2_" + tableName)
            val keyed_result2 = DistHQL(
                    spark, hbaseContext, 
                    "GET FROM " + tableName + " " + 
                    "WITH row2_" + tableName + " " +
                    "WITH_KEYS row2"
                )
                .rdd.map(r => r(0).asInstanceOf[String])
                .filter(s => s != "NA")
                .map(s => s.split(sepStr))
                .map(arr => keyed_output(arr(0), arr.slice(1, arr.length - 1).mkString(sepStr)))
                .toDF()
            if (keyed_result2.count() > 0) {
                val joined_result = rowPairsDF_filt.select($"row2".alias("id"), $"row1")
                    .join(keyed_result2, Seq("id"), "right")
                val trans_result = joined_result.select("row1", "output").rdd
                    .map(r => r(0).asInstanceOf[String] + sepStr + r(1).asInstanceOf[String])
                    .map(s => output_string(s))
                    .toDF()
                trans_result.createOrReplaceTempView("trans_result_" + tableName)
                // write row2 to row1
                DistHQL(
                    spark, hbaseContext,
                    "PUT INTO " + tableName + " " + 
                    "WITH_RECORDS trans_result_" + tableName
                )
                // delete row2
                DistHQL(
                    spark, hbaseContext, 
                    "DELETE FROM " + tableName + " " +
                    "WITH row2_" + tableName + " " +
                    "WITH_KEYS row2"
                )
            }
        }
    }

    // Backward-compatible overload for mergeRows
    def mergeRows(sc: SparkContext, sqlc: SQLContext, hbaseContext: HBaseContext, rowPairsDF: DataFrame, tableName: String): Unit = {
        mergeRows(sqlc.sparkSession, hbaseContext, rowPairsDF, tableName)
    }

    // Backward-compatible overload for DistHQL
    def DistHQL(sc: SparkContext, sqlc: SQLContext, hbaseContext: HBaseContext, HQL_string: String): DataFrame = {
        DistHQL(sqlc.sparkSession, hbaseContext, HQL_string)
    }

    // Primary modern DistHQL accepting SparkSession
    @throws(classOf[Exception])
    def DistHQL(spark: SparkSession, hbaseContext: HBaseContext, HQL_string: String): DataFrame = {
        import spark.implicits._
        val sep = universal_sepStr
        val pb_schema = StructType(Array(StructField("k", StringType, true)))
        val empty_string_df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], pb_schema)
        
        val conf = getHBaseConfiguration()
        val conn = ConnectionFactory.createConnection(conf)
        val admin = conn.getAdmin
        
        // query settings
        val query_components = HQL_string.trim.split("\\s+")
        val query_components_lowercase = query_components.map(_.toLowerCase)
        val nComponents = query_components.length
        val batch_size = default_batch_size
        
        val query_action = query_components_lowercase(0)
        var query_result = empty_string_df
        
        // check reserved words format
        val reserved_checklist = HQL_reserved_words(query_action)
        var reserved_checked = 0
        breakable {
            for (reserved_word <- reserved_checklist) {
                if (query_components_lowercase.contains(reserved_word)) {
                    val reserved_index = query_components_lowercase.indexOf(reserved_word)
                    if (reserved_word.contains("filter") && reserved_index + 2 > query_components_lowercase.length - 1) 
                        break()
                    else if (reserved_index + 1 > query_components_lowercase.length - 1) 
                        break()
                }
                reserved_checked += 1
            }
        }
        if (reserved_checked < reserved_checklist.length)
            throw HBaseHelperException("Format incorrect, please recheck input HQL query.")
            
        // identify target hbase table
        val referHbaseTable_keyword = if (query_action == "put") "into" else if (query_action == "append") "onto" else "from"
        var hbaseTableName = "not yet known"
        if (query_components_lowercase.contains(referHbaseTable_keyword) 
            && query_components_lowercase.indexOf(referHbaseTable_keyword) < nComponents - 1) {
            val refer_hbase_index = query_components_lowercase.indexOf(referHbaseTable_keyword)
            hbaseTableName = query_components(refer_hbase_index + 1)
        } else {
            throw HBaseHelperException(
                "Should attach keyword " +
                referHbaseTable_keywords.mkString(", or ") +
                " before the name of hbase table.")
        }
        
        // identify query specification dataframe
        var querySpecTable: DataFrame = null
        if (!Seq("scan", "put", "append").contains(query_action)) {
            val referQuerySpecTable_keyword = "with"
            if (query_components_lowercase.contains(referQuerySpecTable_keyword) 
                && query_components_lowercase.indexOf(referQuerySpecTable_keyword) < nComponents - 1) {
                val refer_query_spec_index = query_components_lowercase.indexOf(referQuerySpecTable_keyword)
                val querySpecTableName = query_components(refer_query_spec_index + 1)
                querySpecTable = spark.sql("select * from " + querySpecTableName)
            } else {
                throw HBaseHelperException("Should attach keyword 'with' before the name of query table.")
            }
        }
        
        // column family
        var family_str = ""
        if (query_components_lowercase.contains("use_family") && 
            query_components_lowercase.indexOf("use_family") < nComponents - 1) {
            val useFamily_index = query_components_lowercase.indexOf("use_family")
            family_str = query_components(useFamily_index + 1)
        }

        // execute query actions
        if (query_action == "scan") {
            val scan = new Scan()
            val filterList = new FilterList()
            val use_qualifier_filter = query_components_lowercase.contains("use_qualifier_filter")
            val use_value_filter = query_components_lowercase.contains("use_value_filter")
            val use_timestamp_filter = query_components_lowercase.contains("use_timestamp_filter")
            
            if (family_str.nonEmpty) {
                filterList.addFilter(new FamilyFilter(CompareOperator.EQUAL, new RegexStringComparator("^" + family_str + "$")))
            }
            if (use_qualifier_filter) {
                try {
                    val idx = query_components_lowercase.indexOf("use_qualifier_filter")
                    val min_str = query_components(idx + 1)
                    val max_str = query_components(idx + 2)
                    filterList.addFilter(new ColumnRangeFilter(Bytes.toBytes(min_str), true, Bytes.toBytes(max_str), true))
                } catch {
                    case _: Throwable => throw HBaseHelperException("Please provide min & max for qualifier filter")
                }
            }
            if (use_value_filter) {
                try {
                    val idx = query_components_lowercase.indexOf("use_value_filter")
                    val min_str = query_components(idx + 1)
                    val max_str = query_components(idx + 2)
                    filterList.addFilter(new ValueFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(min_str))))
                    filterList.addFilter(new ValueFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(max_str))))
                } catch {
                    case _: Throwable => throw HBaseHelperException("Please provide min & max for value filter")
                }
            }
            if (use_timestamp_filter) {
                val idx = query_components_lowercase.indexOf("use_timestamp_filter")
                try {
                    val min_ts = query_components(idx + 1).toLong
                    val max_ts = query_components(idx + 2).toLong
                    scan.setTimeRange(min_ts, max_ts)
                } catch {
                    case _: Throwable => throw HBaseHelperException("Should give Long formatted timestamp inputs in HQL string.")
                }
            }
            scan.setCaching(batch_size)
            if (filterList.getFilters.size() > 0) {
                scan.setFilter(filterList)
            }
            query_result = hbaseContext.hbaseRDD(TableName.valueOf(hbaseTableName), scan)
                .map(v => v._2)
                .map(result => formatted_string_result(result, "\n"))
                .flatMap(s => s.split("\n"))
                .map(s => output_string(s)).toDF()

        } else if (query_action == "get") {
            var row_input_cols: Seq[String] = Seq()
            val with_keys = query_components_lowercase.contains("with_keys")
            val with_qualifiers = query_components_lowercase.contains("with_qualifiers")
            val with_qualifier_filters = query_components_lowercase.contains("with_qualifier_filters")
            val with_value_filters = query_components_lowercase.contains("with_value_filters")
            
            require(
                (family_str.nonEmpty == with_qualifiers) || (!with_qualifiers),
                "must specify family if qualifiers are specified."
            )
            require(
                (with_qualifiers != with_qualifier_filters) || (!with_qualifiers && !with_qualifier_filters),
                "must use either only 'with_qualifiers' or 'with_qualifier_filters'"
            )
            
            var row_keys_name = ""
            if (with_keys) {
                val idx = query_components_lowercase.indexOf("with_keys")
                if (idx < nComponents - 1) {
                    row_keys_name = query_components(idx + 1)
                    row_input_cols = row_input_cols :+ row_keys_name
                } else throw HBaseHelperException("Please provide name of 'keys' dataset for specific keys")
            }
            
            var qualifiers_name = "" 
            if (with_qualifiers) {
                val idx = query_components_lowercase.indexOf("with_qualifiers")
                if (idx < nComponents - 1) {
                    qualifiers_name = query_components(idx + 1)
                    row_input_cols = row_input_cols :+ qualifiers_name
                } else throw HBaseHelperException("Please provide name of 'qualifiers' dataset for specific keys")
            }
            
            var min_qualifiers_name = ""
            var max_qualifiers_name = ""
            if (with_qualifier_filters) {
                val idx = query_components_lowercase.indexOf("with_qualifier_filters")
                if (idx < nComponents - 2) {
                    min_qualifiers_name = query_components(idx + 1)
                    max_qualifiers_name = query_components(idx + 2)
                    row_input_cols = row_input_cols ++ Seq(min_qualifiers_name, max_qualifiers_name)
                } else throw HBaseHelperException("Please provide min & max for qualifier filter")
            }
            
            var min_values_name = ""
            var max_values_name = ""
            if (with_value_filters) {
                val idx = query_components_lowercase.indexOf("with_value_filters")
                if (idx < nComponents - 2) {
                    min_values_name = query_components(idx + 1)
                    max_values_name = query_components(idx + 2)
                    row_input_cols = row_input_cols ++ Seq(min_values_name, max_values_name)
                } else throw HBaseHelperException("Please provide min & max for value filter")
            }
            
            val row_input = querySpecTable.select(row_input_cols.map(c => col(c)): _*)
            query_result = hbaseContext.bulkGet[Row, String](
                TableName.valueOf(hbaseTableName),
                batch_size,
                row_input.rdd,
                row_input_row => {
                    var get: Get = null
                    try {
                        val filterList = new FilterList()
                        val row_key_str = row_input_row.getAs[String](row_keys_name)
                        get = new Get(Bytes.toBytes(row_key_str))
                        if (family_str.nonEmpty) {
                            get.addFamily(Bytes.toBytes(family_str))
                        }
                        if (with_qualifiers && family_str.nonEmpty && qualifiers_name.nonEmpty) {
                            val qualifier_str = row_input_row.getAs[String](qualifiers_name)
                            get.addColumn(Bytes.toBytes(family_str), Bytes.toBytes(qualifier_str))    
                        }
                        if (with_qualifier_filters) {
                            val min_q = row_input_row.getAs[String](min_qualifiers_name)
                            val max_q = row_input_row.getAs[String](max_qualifiers_name)
                            filterList.addFilter(new ColumnRangeFilter(Bytes.toBytes(min_q), true, Bytes.toBytes(max_q), true))
                        }
                        if (with_value_filters) {
                            val min_v = row_input_row.getAs[String](min_values_name)
                            val max_v = row_input_row.getAs[String](max_values_name)
                            filterList.addFilter(new ValueFilter(CompareOperator.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(min_v))))
                            filterList.addFilter(new ValueFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes(max_v))))
                        }
                        get.setFilter(filterList)
                    } catch {
                        case _: Throwable => throw HBaseHelperException("Error processing row input: " + row_input_row.toString)
                    }
                    get
                },
                (result: Result) => formatted_string_result(result, "\n")
            ).flatMap(s => s.split("\n"))
             .map(s => output_string(s)).toDF()

        } else if (query_action == "delete") {
            var row_input_cols: Seq[String] = Seq()
            var row_keys_name = ""
            if (query_components_lowercase.contains("with_keys") && 
                query_components_lowercase.indexOf("with_keys") < nComponents - 1) {
                val idx = query_components_lowercase.indexOf("with_keys")
                row_keys_name = query_components(idx + 1)
                row_input_cols = row_input_cols :+ row_keys_name
            } else {
                throw HBaseHelperException("Should attach keyword 'with_keys' before the given keys.")
            }                        
            var qualifiers_name = "" 
            if (query_components_lowercase.contains("with_qualifiers") && 
                query_components_lowercase.indexOf("with_qualifiers") < nComponents - 1) {
                val idx = query_components_lowercase.indexOf("with_qualifiers")
                qualifiers_name = query_components(idx + 1)
                row_input_cols = row_input_cols :+ qualifiers_name
            }
            val row_input = querySpecTable.select(row_input_cols.map(c => col(c)): _*)
            hbaseContext.bulkDelete[Row](
                row_input.rdd,
                TableName.valueOf(hbaseTableName),
                row_input_row => {                  
                    val row_key_str = row_input_row.getAs[String](row_keys_name)
                    val del = new Delete(Bytes.toBytes(row_key_str))
                    if (family_str.nonEmpty && qualifiers_name.nonEmpty) {
                        del.addColumn(Bytes.toBytes(family_str), Bytes.toBytes(row_input_row.getAs[String](qualifiers_name)))    
                    } else if (family_str.nonEmpty && qualifiers_name.isEmpty) {
                        del.addFamily(Bytes.toBytes(family_str))
                    }
                    del
                },
                batch_size
            )

        } else if (query_action == "put") {
            var records_name = ""
            if (query_components_lowercase.contains("with_records") &&
                query_components_lowercase.indexOf("with_records") < nComponents - 1) {
                val idx = query_components_lowercase.indexOf("with_records")
                records_name = query_components(idx + 1)
            } else {
                throw HBaseHelperException("Should attach keyword 'with_records' before the given records.")
            }
            val records = spark.sql("select * from " + records_name).rdd
                .map(r => r(0).asInstanceOf[String])
                .map(s => s.split(sep))
                
            val invalidCount = records.filter(_.length != 4).count()
            if (invalidCount > 0) {
                throw HBaseHelperException(
                    "All records in 'put' must be of format 'row :: columnFamily :: columnQualifier :: value'."
                )
            }
            if (query_components_lowercase.contains("use_hfile")) {
                val path_s = "hdfs:///tmp/" + java.util.UUID.randomUUID.toString
                val path = new Path(path_s)
                hbaseContext.bulkLoad(
                    records,
                    TableName.valueOf(hbaseTableName),
                    (r: Array[String]) => {
                        Seq((new KeyFamilyQualifier(Bytes.toBytes(r(0)), Bytes.toBytes(r(1)), Bytes.toBytes(r(2))), Bytes.toBytes(r(3)))).iterator
                    },
                    path_s
                )

                val bulkLoader = BulkLoadHFiles.create(conf)
                val fs = FileSystem.get(conf)
                var repeat = false
                do {
                    try {
                        repeat = false
                        bulkLoader.bulkLoad(TableName.valueOf(hbaseTableName), path)
                    } catch {
                        case e: IOException =>
                            repeat = true
                            e.printStackTrace()
                    }
                } while (repeat)
                fs.delete(path, true)
                fs.close()
            } else {
                hbaseContext.bulkPut[Array[String]](
                    records,
                    TableName.valueOf(hbaseTableName),
                    (putRecord: Array[String]) => {
                        val rowkey = Bytes.toBytes(putRecord(0))
                        val family = Bytes.toBytes(putRecord(1))
                        val qualifier = Bytes.toBytes(putRecord(2))
                        val value = Bytes.toBytes(putRecord(3))
                        val put = new Put(rowkey)
                        put.addColumn(family, qualifier, value)
                    }
                )
            }

        } else if (query_action == "append") {
            registerAggregateToStringSetUdaf(spark, "agg_to_string_set")
                
            var records_name = ""
            if (query_components_lowercase.contains("with_records") &&
                query_components_lowercase.indexOf("with_records") < nComponents - 1) {
                val idx = query_components_lowercase.indexOf("with_records")
                records_name = query_components(idx + 1)
            } else {
                throw HBaseHelperException("Should attach keyword 'with_records' before the given records.")
            }
            val records = spark.sql("select * from " + records_name).rdd
                .map(r => r(0).asInstanceOf[String])
                .map(s => s.split(sep))
                
            val invalidCount = records.filter(_.length != 4).count()
            if (invalidCount > 0) {
                throw HBaseHelperException(
                    "All records in 'append' must be of format 'row :: columnFamily :: columnQualifier :: value'."
                )
            }
            val records_df = records
                .map(arr => (arr(0), arr(1), arr(2), arr(3)))
                .toDF("rowKey", "columnFamily", "columnQualifier", "value")
                .withColumn("oldValue", lit(""))
                .select("rowKey", "columnFamily", "columnQualifier", "oldValue", "value")
            
            val old_records_df = hbaseContext.bulkGet[Row, String](
                TableName.valueOf(hbaseTableName),
                batch_size,
                records_df.rdd,
                record_row => {
                    val rk_str = record_row.getAs[String]("rowKey")
                    val cf_str = record_row.getAs[String]("columnFamily")
                    val cq_str = record_row.getAs[String]("columnQualifier")
                    val get = new Get(Bytes.toBytes(rk_str))
                    get.addFamily(Bytes.toBytes(cf_str))
                    get.addColumn(Bytes.toBytes(cf_str), Bytes.toBytes(cq_str))    
                    get
                },
                (result: Result) => formatted_string_result(result, "\n")
            ).flatMap(s => s.split("\n"))
                .filter(s => s != "NA")
                .map(s => s.split(sep))
                .map(arr => (arr(0), arr(1), arr(2), arr(3)))
                .toDF("rowKey", "columnFamily", "columnQualifier", "oldValue")
                .withColumn("value", lit(""))
                .select("rowKey", "columnFamily", "columnQualifier", "oldValue", "value")
            
            val union_records_df = records_df.union(old_records_df)
            val merged_records_df = union_records_df
                .map(r => 
                    (
                        r.getAs[String]("rowKey") + sep + r.getAs[String]("columnFamily") + sep + r.getAs[String]("columnQualifier"),
                        r.getAs[String]("oldValue"),
                        r.getAs[String]("value")
                    )
                )
                .toDF("rowColfColq", "oldValue", "value")
                .groupBy("rowColfColq")
                .agg(
                    expr("agg_to_string_set(oldValue)").alias("oldValue"),
                    expr("agg_to_string_set(value)").alias("value")
                )
                .map(r => 
                    (
                        r.getAs[String]("rowColfColq"),
                        r.getAs[Seq[String]]("oldValue").filter(_.nonEmpty),
                        r.getAs[Seq[String]]("value").filter(_.nonEmpty)
                    )
                )
                .toDF("rowColfColq", "oldValue", "value")
                .map(r => (r.getAs[String]("rowColfColq"), r.getAs[Seq[String]]("oldValue") ++ r.getAs[Seq[String]]("value")))
                .map(t => (t._1, t._2.mkString("")))
                .toDF("rowColfColq", "mergedValue")
                .map(r => r.getAs[String]("rowColfColq") + sep + r.getAs[String]("mergedValue"))
                .map(s => s.split(sep))
                .map(arr => arr.mkString(sep))
                .toDF("merged")
                
            merged_records_df.createOrReplaceTempView("merged_records_df")
            DistHQL(
                spark, hbaseContext,
                "PUT INTO " + hbaseTableName + " " + 
                "WITH_RECORDS merged_records_df"
            )
        } else {
            throw HBaseHelperException("Action in given HQL string is not recognized.")
        }
        query_result
    }
    
    def tableAvailable(tableName: String): Boolean = {   
        val conf = getHBaseConfiguration()
        val conn = ConnectionFactory.createConnection(conf)
        try {
            val admin = conn.getAdmin
            try {
                val tn = TableName.valueOf(tableName)
                admin.tableExists(tn) && admin.isTableEnabled(tn)
            } finally {
                admin.close()
            }
        } finally {
            conn.close()
        }
    }
}
