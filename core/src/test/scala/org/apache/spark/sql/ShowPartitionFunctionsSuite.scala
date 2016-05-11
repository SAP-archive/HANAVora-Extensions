package org.apache.spark.sql

import com.sap.spark.dstest.DefaultSource
import org.apache.spark.sql.sources.Stride
import org.apache.spark.util.PartitioningFunctionUtils
import org.scalatest.FunSuite

class ShowPartitionFunctionsSuite
  extends FunSuite
  with GlobalSapSQLContext
  with PartitioningFunctionUtils {

  override def beforeEach(): Unit = {
    super.beforeEach()
    DefaultSource.reset()
  }

  test("Show partition functions shows no partitioning functions if none are there") {
    val funs = sqlc.sql("SHOW PARTITION FUNCTIONS USING com.sap.spark.dstest").collect()

    assert(funs.isEmpty)
  }

  // scalastyle:off magic.number
  test("Show partition functions shows the previously registered partitioning functions") {
    createHashPartitioningFunction("foo", Seq("string", "float"), Some(10), "com.sap.spark.dstest")
    createRangePartitioningFunction("bar", "int", 0, 10, Stride(10), "com.sap.spark.dstest")
    createRangeSplitPartitioningFunction("baz", "float", Seq(1, 2, 3),
      rightClosed = true, "com.sap.spark.dstest")

    val funs = sqlc.sql("SHOW PARTITION FUNCTIONS USING com.sap.spark.dstest").collect()

    assertResult(Set(
      Row("baz", "RangeSplitPartitioningFunction", "FloatType", "1,2,3",
        true, null, null, null, null, null),
      Row("foo", "HashPartitioningFunction", "StringType,FloatType", null,
        null, null, null, null, null, 10),
      Row("bar", "RangeIntervalPartitioningFunction", "IntegerType", null, null,
        0, 10, "Stride", 10, null)))(funs.toSet)
  }
  // scalastyle:on magic.number

  // scalastyle:off magic.number
  test("Show partition functions does not show deleted functions") {
    createHashPartitioningFunction("foo", Seq("string", "float"), Some(10), "com.sap.spark.dstest")
    createRangePartitioningFunction("bar", "int", 0, 10, Stride(10), "com.sap.spark.dstest")
    createRangeSplitPartitioningFunction("baz", "float", Seq(1, 2, 3),
      rightClosed = true, "com.sap.spark.dstest")

    val f1 = sqlc.sql("SHOW PARTITION FUNCTIONS USING com.sap.spark.dstest").collect()

    assertResult(Set(
      Row("baz", "RangeSplitPartitioningFunction", "FloatType", "1,2,3",
        true, null, null, null, null, null),
      Row("foo", "HashPartitioningFunction", "StringType,FloatType", null,
        null, null, null, null, null, 10),
      Row("bar", "RangeIntervalPartitioningFunction", "IntegerType", null, null,
        0, 10, "Stride", 10, null)))(f1.toSet)

    dropPartitioningFunction("bar", dataSource = "com.sap.spark.dstest")

    val f2 = sqlc.sql("SHOW PARTITION FUNCTIONS USING com.sap.spark.dstest").collect()

    assertResult(Set(
      Row("baz", "RangeSplitPartitioningFunction", "FloatType", "1,2,3",
        true, null, null, null, null, null),
      Row("foo", "HashPartitioningFunction", "StringType,FloatType", null,
        null, null, null, null, null, 10)))(f2.toSet)
  }
  // scalastyle:on magic.number
}

