package org.apache.spark.sql

import java.io.File

import com.sap.spark.{GlobalSparkContext, WithSQLContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Cast}
import org.apache.spark.sql.types.{UTF8String, StringType, StructType}
import org.scalatest.Suite

import scala.io.Source

trait GlobalSapSQLContext extends GlobalSparkContext with WithSQLContext {
  self: Suite =>

  override def sqlContext: SQLContext = GlobalSapSQLContext._sqlc

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
      val stringRow = Row.fromSeq(line.split(",", -1).map(UTF8String(_)))
      Row.fromSeq(conversions.map({ c => c.eval(stringRow) }))
    })
    val rdd = sc.parallelize(data.toSeq)
    sqlContext.createDataFrame(rdd, sparkSchema)
  }
}



object GlobalSapSQLContext {

  private var _sqlc: SapSQLContext = _

  private def init(sc: SparkContext): Unit = {
    if (_sqlc == null) {
      _sqlc = new SapSQLContext(sc)
    }
  }

  private def reset(): Unit = {
    _sqlc.catalog.unregisterAllTables()
  }

}


trait GlobalVelocitySQLContext extends GlobalSparkContext with WithSQLContext {
  self: Suite =>
  override def sqlContext: SQLContext = GlobalVelocitySQLContext._sqlc

  override protected def setUpSQLContext(): Unit =
    GlobalVelocitySQLContext.init(sc)

  override protected def tearDownSQLContext(): Unit =
    GlobalVelocitySQLContext.reset()
}

object GlobalVelocitySQLContext {

  private var _sqlc: SapSQLContext = _

  private def init(sc: SparkContext): Unit = {
    if (_sqlc == null) {
      _sqlc = new SapSQLContext(sc)
    }
  }

  private def reset(): Unit = {
    _sqlc.catalog.unregisterAllTables()
  }

}




