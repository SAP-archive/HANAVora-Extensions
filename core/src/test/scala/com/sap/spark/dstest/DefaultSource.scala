package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{TemporaryAndPersistentRelationProvider, BaseRelation, SchemaRelationProvider, RelationProvider}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

/**
 * Test default source that is capable of creating dummy temporary and persistent relations
 */
class DefaultSource extends SchemaRelationProvider
      with TemporaryAndPersistentRelationProvider {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
                              new DummyRelationWithoutTempFlag(sqlContext, schema)


  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              isTemporary: Boolean): BaseRelation =
                              new DummyRelationWithTempFlag(sqlContext,
                                StructType(Seq(StructField("field", IntegerType, true))),
                                isTemporary)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              sparkSchema: StructType, isTemporary: Boolean): BaseRelation =
                              new DummyRelationWithTempFlag(sqlContext, sparkSchema, isTemporary)

}
