package HbaseSparkQL

// general imports
import scala.collection.immutable.List
import scala.util.control._
import scala.util.Sorting

// JSON4S (replaces legacy net.liftweb.json)
import org.json4s._
import org.json4s.jackson.JsonMethods._

// math
import scala.math.pow

// modern Spark ML linear algebra (replaces mllib)
import org.apache.spark.ml.linalg.{Vector, Vectors, DenseVector, SparseVector}

package object FeatureHelper {

    @throws(classOf[Exception])
    def dense_vector_addition(vec1: Vector, vec2: Vector): Vector = {        
        if (!vec1.isInstanceOf[DenseVector] || !vec2.isInstanceOf[DenseVector]) {
            throw new IllegalArgumentException("Parameters must belong to class DenseVector!")
        }
        if (vec1.size != vec2.size) {
            throw new IllegalArgumentException(s"Given vectors have different sizes: ${vec1.size} vs ${vec2.size}")
        }
        if (vec1.size == 0) {
            vec2
        } else if (vec2.size == 0) {
            vec1
        } else {
            val newVec = new Array[Double](vec1.size)
            var i = 0
            while (i < vec1.size) {
                newVec(i) = vec1(i) + vec2(i)
                i += 1
            }
            Vectors.dense(newVec)
        }
    }

    @throws(classOf[Exception])
    def dense_vector_subtraction(vec1: Vector, vec2: Vector): Vector = {
        if (!vec1.isInstanceOf[DenseVector] || !vec2.isInstanceOf[DenseVector]) {
            throw new IllegalArgumentException("Parameters must belong to class DenseVector!")
        }
        if (vec1.size != vec2.size) {
            throw new IllegalArgumentException(s"Given vectors have different sizes: ${vec1.size} vs ${vec2.size}")
        }
        if (vec1.size == 0) {
            vec2
        } else if (vec2.size == 0) {
            vec1
        } else {
            val newVec = new Array[Double](vec1.size)
            var i = 0
            while (i < vec1.size) {
                newVec(i) = vec1(i) - vec2(i)
                i += 1
            }
            Vectors.dense(newVec)
        }
    }

    // filter the document with all filters in filterlist
    // each filter consists of three parts: <1st arg> <operator> <2nd arg>
    @throws(classOf[Exception])
    def filter_multiple(doc: JValue, filter_list: List[String]): Boolean = {
        for (filt <- filter_list) {
            val filt_str = filt.toString
            val filt_parse = filt_str.split("\\s+")
            
            val compared = filt_parse(0)
            val oper = filt_parse(1)
            val compare_val = if (filt_parse.length == 3) filt_parse(2) else "NA"

            val f_val_j = get_feature_val(doc, compared)
            val f_val_str = f_val_j match {
                case JString(s) => s
                case JInt(n) => n.toString
                case JDouble(d) => d.toString
                case JDecimal(d) => d.toString
                case JBool(b) => b.toString
                case _ => f_val_j.values.toString
            }
            
            var isContained = true
            if (oper == "eq") { 
                isContained = (f_val_str == compare_val)
            } else if (oper == "ne") {
                isContained = (f_val_str != compare_val)
            } else if (oper == "lt") {
                isContained = f_val_str.toFloat < compare_val.toFloat
            } else if (oper == "gt") {
                isContained = f_val_str.toFloat > compare_val.toFloat
            } else if (oper == "not_empty") {
                isContained = (f_val_str != "NA" && f_val_str.nonEmpty)
            } else {
                throw new IllegalArgumentException(s"Incorrect filter parameter operator: $oper")
            }
            if (!isContained) {
                return false
            }
        }
        true
    }

    // get the feature value of the given f_name (feature name) in the given json document
    def get_feature_val(doc: JValue, f_name: String): JValue = {
        val f_list = f_name.split('.')
        var f_val = doc
        var i = 0
        
        while (i < f_list.length) {
            val f = f_list(i)
            f_val = f_val \ f
            if (f_val == JNothing || f_val == JNull) {
                return JString("NA")
            }
            i += 1
        }

        f_val
    }

    @throws(classOf[Exception])
    def sparse_vector_addition(vec1: Vector, vec2: Vector): Vector = {
        if (!vec1.isInstanceOf[SparseVector] || !vec2.isInstanceOf[SparseVector]) {
            throw new IllegalArgumentException("Parameters must belong to class SparseVector!")
        }
        val sVec1 = vec1.asInstanceOf[SparseVector]
        val sVec2 = vec2.asInstanceOf[SparseVector]
        val size1 = sVec1.size
        val size2 = sVec2.size
    
        if (size1 != size2) {
            throw new IllegalArgumentException(s"Given vectors have different sizes: $size1 vs $size2")
        }
        val indices1 = sVec1.indices
        val indices2 = sVec2.indices
        val values1 = sVec1.values
        val values2 = sVec2.values
        val pairs1 = indices1 zip values1
        val pairs2 = indices2 zip values2
        Sorting.quickSort(pairs1)
        Sorting.quickSort(pairs2)
    
        val newSize = size1
        val newIndices = (indices1.toSet union indices2.toSet).toArray.sorted
        val newValues = new Array[Double](newIndices.length)
        var i = 0
        for (index <- newIndices) {
            var newValue = 0.0
            val idx1 = indices1.indexOf(index)
            if (idx1 >= 0) newValue += values1(idx1)
            val idx2 = indices2.indexOf(index)
            if (idx2 >= 0) newValue += values2(idx2)
            newValues(i) = newValue
            i += 1
        }
    
        Vectors.sparse(newSize, newIndices, newValues)
    }
    
    @throws(classOf[Exception])
    def sparse_vector_subtraction(vec1: Vector, vec2: Vector): Vector = {
        if (!vec1.isInstanceOf[SparseVector] || !vec2.isInstanceOf[SparseVector]) {
            throw new IllegalArgumentException("Parameters must belong to class SparseVector!")
        }
        val sVec1 = vec1.asInstanceOf[SparseVector]
        val sVec2 = vec2.asInstanceOf[SparseVector]
        val size1 = sVec1.size
        val size2 = sVec2.size
    
        if (size1 != size2) {
            throw new IllegalArgumentException(s"Given vectors have different sizes: $size1 vs $size2")
        }
        val indices1 = sVec1.indices
        val indices2 = sVec2.indices
        val values1 = sVec1.values
        val values2 = sVec2.values
        val pairs1 = indices1 zip values1
        val pairs2 = indices2 zip values2
        Sorting.quickSort(pairs1)
        Sorting.quickSort(pairs2)
    
        val newSize = size1
        val newIndices = (indices1.toSet union indices2.toSet).toArray.sorted
        val newValues = new Array[Double](newIndices.length)
        var i = 0
        for (index <- newIndices) {
            var newValue = 0.0
            val idx1 = indices1.indexOf(index)
            if (idx1 >= 0) newValue += values1(idx1)
            val idx2 = indices2.indexOf(index)
            if (idx2 >= 0) newValue -= values2(idx2)
            newValues(i) = newValue
            i += 1
        }

        Vectors.sparse(newSize, newIndices, newValues)
    }

    // addition of two vectors
    @throws(classOf[Exception])
    def vector_addition(vec1: Vector, vec2: Vector): Vector = (vec1, vec2) match {
        case (a: SparseVector, b: SparseVector) => sparse_vector_addition(vec1, vec2)
        case (a: DenseVector, b: DenseVector) => dense_vector_addition(vec1, vec2)
        case _ => throw new IllegalArgumentException("Type mismatch or Non-Vector input!")
    }

    // subtraction of two vectors
    @throws(classOf[Exception])
    def vector_subtraction(vec1: Vector, vec2: Vector): Vector = (vec1, vec2) match {
        case (a: SparseVector, b: SparseVector) => sparse_vector_subtraction(vec1, vec2)
        case (a: DenseVector, b: DenseVector) => dense_vector_subtraction(vec1, vec2)
        case _ => throw new IllegalArgumentException("Type mismatch or Non-Vector input!")
    }
    
    @throws(classOf[Exception])
    def sparse_vector_divide_by_constant(vec: Vector, C: Double): Vector = {
        if (!vec.isInstanceOf[SparseVector]) {
            throw new IllegalArgumentException("Parameter must belong to class SparseVector!")
        }
        val sVec = vec.asInstanceOf[SparseVector]
        val size = sVec.size
        val indices = sVec.indices
        val values = sVec.values
        val newValues = values.map(value => value / C)
        Vectors.sparse(size, indices, newValues)
    }
    
    // divide a vector by a constant C
    def vector_divide_by_constant(vec: Vector, C: Double): Vector = vec match {
        case s: SparseVector => sparse_vector_divide_by_constant(vec, C)
        case d: DenseVector => Vectors.dense(d.toArray.map(value => value / C))
        case _ => throw new IllegalArgumentException("Type mismatch or Non-Vector input!")
    }
    
    implicit class PowerInt(i: Int) {
        def ** (b: Int): Int = pow(i.toDouble, b.toDouble).toInt
    }
}
