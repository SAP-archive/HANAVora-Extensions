package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{DropRelation, TemporaryFlagRelation, BaseRelation}
import org.apache.spark.sql.types.StructType

/**
 * Test relation with the temporary and non temporary flags
 */
class DummyRelationWithTempFlag(val sqlContext: SQLContext, val schema : StructType,
                                val temporary : Boolean)
  extends BaseRelation
  with TemporaryFlagRelation
  with DropRelation {

  override def isTemporary(): Boolean = temporary

  override def dropTable(): Unit = {}
}

class DummyRelationWithoutTempFlag(val sqlContext: SQLContext, val schema : StructType)
  extends BaseRelation
  with DropRelation  {

  override def dropTable(): Unit = {}

}
