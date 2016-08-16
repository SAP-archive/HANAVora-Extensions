package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.commands.WithOrigin
import org.apache.spark.sql.sources.{BaseRelation, DropRelation, Table}
import org.apache.spark.sql.types._

/**
 * Test relation with the temporary and non temporary flags
 */
case class DummyRelationWithTempFlag(
    sqlContext: SQLContext,
    tableName: Seq[String],
    schema: StructType,
    temporary: Boolean)
  extends BaseRelation
  with Table
  with DropRelation
  with WithOrigin {

  override val provider: String = "com.sap.spark.dstest"

  override def isTemporary: Boolean = temporary

  override def dropTable(): Unit = {}
}

case class DummyRelationWithoutTempFlag(
    sqlContext: SQLContext,
    schema: StructType)
  extends BaseRelation
  with DropRelation
  with Table
  with WithOrigin {

  override def isTemporary: Boolean = false

  override val provider: String = "com.sap.spark.dstest"

  override def dropTable(): Unit = {}
}
