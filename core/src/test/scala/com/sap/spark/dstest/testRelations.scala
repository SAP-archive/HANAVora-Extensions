package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.commands.Table
import org.apache.spark.sql.sources.{BaseRelation, DropRelation, TemporaryFlagRelation}
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
  with TemporaryFlagRelation
  with DropRelation
  with Table {

  override def isTemporary(): Boolean = temporary

  override def dropTable(): Unit = {}
}

case class DummyRelationWithoutTempFlag(
    sqlContext: SQLContext,
    schema: StructType)
  extends BaseRelation
  with DropRelation
  with Table {

  override def dropTable(): Unit = {}
}
