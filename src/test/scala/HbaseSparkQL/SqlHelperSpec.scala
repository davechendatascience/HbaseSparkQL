package HbaseSparkQL

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.linalg.Vectors
import HbaseSparkQL.SqlHelper._

class SqlHelperSpec extends AnyFlatSpec with Matchers {

    "VectorSumAggregator" should "aggregate vectors correctly" in {
        val agg = new VectorSumAggregator(vectorSize = 3)
        var buffer = agg.zero
        buffer = agg.reduce(buffer, Vectors.dense(1.0, 2.0, 3.0))
        buffer = agg.reduce(buffer, Vectors.dense(4.0, 5.0, 6.0))
        val result = agg.finish(buffer)
        result.toArray shouldBe Array(5.0, 7.0, 9.0)
    }

    "VectorMeanAggregator" should "calculate mean of vectors correctly" in {
        val agg = new VectorMeanAggregator(vectorSize = 3)
        var buffer = agg.zero
        buffer = agg.reduce(buffer, Vectors.dense(2.0, 4.0, 6.0))
        buffer = agg.reduce(buffer, Vectors.dense(4.0, 6.0, 8.0))
        val result = agg.finish(buffer)
        result.toArray shouldBe Array(3.0, 5.0, 7.0)
    }

    "SortSecondElementAggregator" should "sort tuple elements by the second double value" in {
        val agg = new SortSecondElementAggregator
        var buffer = agg.zero
        buffer = agg.reduce(buffer, TupleElement("c", 3.0))
        buffer = agg.reduce(buffer, TupleElement("a", 1.0))
        buffer = agg.reduce(buffer, TupleElement("b", 2.0))
        val result = agg.finish(buffer)
        result.map(_.first) shouldBe Seq("a", "b", "c")
    }
}
