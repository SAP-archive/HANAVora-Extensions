package org.apache.spark.sql

import java.io.File

import com.sap.spark.util.TestUtils
import com.sap.spark.{GlobalSparkContext, WithSQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.unsafe.types._
import org.apache.spark.sql.types._
import org.scalatest.Suite

import scala.io.Source

trait GlobalSapSQLContext extends GlobalSparkContext with WithSQLContext {
  self: Suite =>

  override implicit def sqlContext: SQLContext = GlobalSapSQLContext._sqlc

  override protected def setUpSQLContext(): Unit =
    GlobalSapSQLContext.init(sc)

  override protected def tearDownSQLContext(): Unit =
    GlobalSapSQLContext.reset()

  def getDataFrameFromSourceFile(sparkSchema: StructType, path: File): DataFrame = {
    val conversions = sparkSchema.toSeq.zipWithIndex.map({
      case (field, index) =>
        Cast(BoundReference(index, StringType, nullable = true), field.dataType)
    })
    val data = Source.fromFile(path)
      .getLines()
      .map({ line =>
      val stringRow = InternalRow.fromSeq(line.split(",", -1).map(UTF8String.fromString))
      Row.fromSeq(conversions.map({ c => c.eval(stringRow) }))
    })
    val rdd = sc.parallelize(data.toSeq, numberOfSparkWorkers)
    sqlContext.createDataFrame(rdd, sparkSchema)
  }
}

object GlobalSapSQLContext {

  private var _sqlc: SQLContext = _

  private def init(sc: SparkContext): Unit =
    if (_sqlc == null) {
      _sqlc = TestUtils.newSQLContext(sc)
    }

  private def reset(): Unit = {
    if (_sqlc != null) {
      _sqlc.catalog.unregisterAllTables()
    }
  }

}
