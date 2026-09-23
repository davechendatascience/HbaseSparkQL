package HbaseSparkQL

import HbaseSparkQL.FeatureHelper.{vector_addition}
import org.apache.spark.ml.linalg.{Vector, Vectors, VectorUDT, SparseVector, DenseVector}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.udaf
import org.apache.spark.sql.types._
import scala.collection.mutable.WrappedArray
import scala.language.implicitConversions

package object SqlHelper {
    
    // perform zipWithIndex on DataFrame's like what you can do with RDD's
    def dfZipWithIndex(
        df: DataFrame,
        offset: Long = 1L,
        colName: String = "id",
        inFront: Boolean = true
    ): DataFrame = {
        val spark = df.sparkSession
        val rddIndexed = df.rdd.zipWithIndex().map { case (row, idx) =>
            val newIdx = idx + offset
            val values = row.toSeq
            val newRow = if (inFront) Seq(newIdx) ++ values else values ++ Seq(newIdx)
            Row.fromSeq(newRow)
        }
        val idField = StructField(colName, LongType, nullable = false)
        val schema = if (inFront) {
            StructType(Array(idField) ++ df.schema.fields)
        } else {
            StructType(df.schema.fields ++ Array(idField))
        }
        spark.createDataFrame(rddIndexed, schema)
    }

    // --- Vector Sum Aggregator ---
    case class VectorSumState(var sum: Array[Double], var initialized: Boolean)

    class VectorSumAggregator(val vectorSize: Int = 5851) extends Aggregator[Vector, VectorSumState, Vector] {
        override def zero: VectorSumState = VectorSumState(new Array[Double](vectorSize), initialized = false)

        override def reduce(buffer: VectorSumState, input: Vector): VectorSumState = {
            if (input != null) {
                if (!buffer.initialized) {
                    val inputArr = input.toArray
                    val copyLen = math.min(buffer.sum.length, inputArr.length)
                    Array.copy(inputArr, 0, buffer.sum, 0, copyLen)
                    buffer.initialized = true
                } else {
                    val inputArr = input.toArray
                    var i = 0
                    val limit = math.min(buffer.sum.length, inputArr.length)
                    while (i < limit) {
                        buffer.sum(i) += inputArr(i)
                        i += 1
                    }
                }
            }
            buffer
        }

        override def merge(b1: VectorSumState, b2: VectorSumState): VectorSumState = {
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

        override def finish(reduction: VectorSumState): Vector = {
            Vectors.dense(reduction.sum)
        }

        override def bufferEncoder: Encoder[VectorSumState] = Encoders.product[VectorSumState]
        override def outputEncoder: Encoder[Vector] = ExpressionEncoder[Vector]()
    }

    // --- Vector Mean Aggregator ---
    case class VectorMeanState(var sum: Array[Double], var count: Long, var initialized: Boolean)

    class VectorMeanAggregator(val vectorSize: Int = 5851) extends Aggregator[Vector, VectorMeanState, Vector] {
        override def zero: VectorMeanState = VectorMeanState(new Array[Double](vectorSize), 0L, initialized = false)

        override def reduce(buffer: VectorMeanState, input: Vector): VectorMeanState = {
            if (input != null) {
                if (!buffer.initialized) {
                    val inputArr = input.toArray
                    val copyLen = math.min(buffer.sum.length, inputArr.length)
                    Array.copy(inputArr, 0, buffer.sum, 0, copyLen)
                    buffer.count = 1L
                    buffer.initialized = true
                } else {
                    val inputArr = input.toArray
                    var i = 0
                    val limit = math.min(buffer.sum.length, inputArr.length)
                    while (i < limit) {
                        buffer.sum(i) += inputArr(i)
                        i += 1
                    }
                    buffer.count += 1L
                }
            }
            buffer
        }

        override def merge(b1: VectorMeanState, b2: VectorMeanState): VectorMeanState = {
            if (!b1.initialized) b2
            else if (!b2.initialized) b1
            else {
                var i = 0
                while (i < b1.sum.length && i < b2.sum.length) {
                    b1.sum(i) += b2.sum(i)
                    i += 1
                }
                b1.count += b2.count
                b1
            }
        }

        override def finish(reduction: VectorMeanState): Vector = {
            if (reduction.count == 0L) {
                Vectors.dense(reduction.sum)
            } else {
                val meanArr = reduction.sum.map(_ / reduction.count.toDouble)
                Vectors.dense(meanArr)
            }
        }

        override def bufferEncoder: Encoder[VectorMeanState] = Encoders.product[VectorMeanState]
        override def outputEncoder: Encoder[Vector] = ExpressionEncoder[Vector]()
    }

    // --- Sort Second Element Aggregator ---
    case class TupleElement(first: String, second: Double)

    class SortSecondElementAggregator extends Aggregator[TupleElement, Seq[TupleElement], Seq[TupleElement]] {
        override def zero: Seq[TupleElement] = Seq.empty[TupleElement]

        override def reduce(buffer: Seq[TupleElement], input: TupleElement): Seq[TupleElement] = {
            if (input != null) buffer :+ input else buffer
        }

        override def merge(b1: Seq[TupleElement], b2: Seq[TupleElement]): Seq[TupleElement] = {
            b1 ++ b2
        }

        override def finish(reduction: Seq[TupleElement]): Seq[TupleElement] = {
            reduction.sortBy(_.second)
        }

        override def bufferEncoder: Encoder[Seq[TupleElement]] = Encoders.product[Seq[TupleElement]]
        override def outputEncoder: Encoder[Seq[TupleElement]] = Encoders.product[Seq[TupleElement]]
    }

    // --- Aggregate To Set Aggregator ---
    class AggregateToSetAggregator[T: Encoder] extends Aggregator[T, Set[T], Seq[T]] {
        override def zero: Set[T] = Set.empty[T]

        override def reduce(buffer: Set[T], input: T): Set[T] = {
            if (input != null) buffer + input else buffer
        }

        override def merge(b1: Set[T], b2: Set[T]): Set[T] = {
            b1 ++ b2
        }

        override def finish(reduction: Set[T]): Seq[T] = {
            reduction.toSeq
        }

        override def bufferEncoder: Encoder[Set[T]] = ExpressionEncoder[Set[T]]()
        override def outputEncoder: Encoder[Seq[T]] = ExpressionEncoder[Seq[T]]()
    }

    // Convenience UDF registration helpers for Spark 3.5
    def registerVectorSumUdaf(spark: SparkSession, name: String = "vector_sum", size: Int = 5851): Unit = {
        spark.udf.register(name, udaf(new VectorSumAggregator(size)))
    }

    def registerVectorMeanUdaf(spark: SparkSession, name: String = "vector_mean", size: Int = 5851): Unit = {
        spark.udf.register(name, udaf(new VectorMeanAggregator(size)))
    }

    def registerAggregateToStringSetUdaf(spark: SparkSession, name: String = "agg_to_string_set"): Unit = {
        implicit val stringEncoder: Encoder[String] = Encoders.STRING
        spark.udf.register(name, udaf(new AggregateToSetAggregator[String]))
    }
}
