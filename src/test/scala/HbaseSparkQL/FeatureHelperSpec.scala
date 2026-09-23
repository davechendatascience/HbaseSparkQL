package HbaseSparkQL

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.ml.linalg.{Vectors, DenseVector, SparseVector}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import HbaseSparkQL.FeatureHelper._

class FeatureHelperSpec extends AnyFlatSpec with Matchers {

    "FeatureHelper.dense_vector_addition" should "add two dense vectors element-wise" in {
        val v1 = Vectors.dense(1.0, 2.0, 3.0)
        val v2 = Vectors.dense(4.0, 5.0, 6.0)
        val sum = dense_vector_addition(v1, v2)
        sum.toArray shouldBe Array(5.0, 7.0, 9.0)
    }

    "FeatureHelper.dense_vector_subtraction" should "subtract two dense vectors element-wise" in {
        val v1 = Vectors.dense(5.0, 7.0, 9.0)
        val v2 = Vectors.dense(1.0, 2.0, 3.0)
        val diff = dense_vector_subtraction(v1, v2)
        diff.toArray shouldBe Array(4.0, 5.0, 6.0)
    }

    "FeatureHelper.sparse_vector_addition" should "add two sparse vectors correctly" in {
        val v1 = Vectors.sparse(5, Array(0, 2), Array(1.0, 3.0))
        val v2 = Vectors.sparse(5, Array(1, 2), Array(2.0, 4.0))
        val sum = sparse_vector_addition(v1, v2)
        sum.toArray shouldBe Array(1.0, 2.0, 7.0, 0.0, 0.0)
    }

    "FeatureHelper.sparse_vector_subtraction" should "subtract two sparse vectors correctly" in {
        val v1 = Vectors.sparse(5, Array(0, 2), Array(5.0, 7.0))
        val v2 = Vectors.sparse(5, Array(1, 2), Array(2.0, 3.0))
        val diff = sparse_vector_subtraction(v1, v2)
        diff.toArray shouldBe Array(5.0, -2.0, 4.0, 0.0, 0.0)
    }

    "FeatureHelper.vector_divide_by_constant" should "divide vectors by scalar" in {
        val v = Vectors.dense(2.0, 4.0, 6.0)
        val res = vector_divide_by_constant(v, 2.0)
        res.toArray shouldBe Array(1.0, 2.0, 3.0)
    }

    "FeatureHelper.get_feature_val" should "extract nested json attributes" in {
        val jsonStr = """{"user": {"profile": {"age": "25", "city": "Seattle"}}}"""
        val json = parse(jsonStr)
        val age = get_feature_val(json, "user.profile.age")
        age shouldBe JString("25")

        val missing = get_feature_val(json, "user.profile.country")
        missing shouldBe JString("NA")
    }

    "FeatureHelper.filter_multiple" should "correctly filter json documents" in {
        val jsonStr = """{"age": "25", "city": "Seattle"}"""
        val json = parse(jsonStr)
        val filters = List("age eq 25", "city eq Seattle")
        filter_multiple(json, filters) shouldBe true

        val failingFilters = List("age eq 30")
        filter_multiple(json, failingFilters) shouldBe false
    }

    "PowerInt" should "calculate integer power" in {
        (2 ** 3) shouldBe 8
        (3 ** 2) shouldBe 9
    }
}
