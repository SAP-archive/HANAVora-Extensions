package com.sap.spark.catalystSourceTest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, TemporaryAndPersistentSchemaRelationProvider}
import org.apache.spark.sql.types.StructType

class DefaultSource extends TemporaryAndPersistentSchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, tableName: Seq[String],
                              parameters: Map[String, String],
                              schema: StructType, isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new CatalystSourceTestRelation(parameters, schema, isTemporary, allowExisting)(sqlContext)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    createRelation(sqlContext, Seq.empty, parameters, schema, true, false)
}
