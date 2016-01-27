package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Test default source that is capable of creating dummy temporary and persistent relations
 */
class DefaultSource extends TemporaryAndPersistentSchemaRelationProvider
with TemporaryAndPersistentRelationProvider
with PartitionedRelationProvider
with PartitioningFunctionProvider
with RegisterAllTableRelations {


  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, None, None, isTemporary = false, allowExisting = false)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new DummyRelationWithoutTempFlag(sqlContext, schema)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext,
      DefaultSource.standardSchema,
      isTemporary)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              sparkSchema: StructType, isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext, sparkSchema, isTemporary)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]], isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext,
      DefaultSource.standardSchema,
      isTemporary)

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String],
                              sparkSchema: StructType, partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]], isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext, sparkSchema, isTemporary)

  override def getAllTableRelations(sqlContext: SQLContext,
                                    options: Map[String, String]): Map[String, BaseRelation] = {
    DefaultSource.relations.map(name =>
      (name, new DummyRelationWithTempFlag(sqlContext, DefaultSource.standardSchema, false))
    ).toMap
  }


  override def getTableRelation(tableName: String, sqlContext: SQLContext,
                                options: Map[String, String]): Option[BaseRelation] = {
    if (DefaultSource.relations.contains(tableName)) {
      Some(new DummyRelationWithTempFlag(sqlContext, DefaultSource.standardSchema, false))
    } else {
      None
    }
  }

  override def createPartitioningFunction(sqlContext: SQLContext,
                                          parameters: Map[String, String],
                                          name: String,
                                          datatypes: Seq[DataType],
                                          definition: String,
                                          partitionsNo: Option[Int]): Unit = {
    // nop
  }

}

/**
 * Companion Object handling already existing relations
 */
object DefaultSource {

  private val standardSchema = StructType(Seq(StructField("field", IntegerType, nullable = true)))

  private var relations = Seq.empty[String]

  def addRelation(name: String): Unit = {
    relations = relations ++ Seq(name)
  }

  def reset(): Unit = {
    relations = Seq.empty[String]
  }

}
