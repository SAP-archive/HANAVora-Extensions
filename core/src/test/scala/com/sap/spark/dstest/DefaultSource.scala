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
with RegisterAllTableRelations
with DatasourceCatalog {


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
                                    options: Map[String, String])
    : Map[String, LogicalPlanSource] = {
    DefaultSource.relations.map(name =>
      (name, BaseRelationSource(
        new DummyRelationWithTempFlag(sqlContext, DefaultSource.standardSchema, false)))
    ).toMap
  }


  override def getTableRelation(tableName: String, sqlContext: SQLContext,
                                options: Map[String, String]): Option[LogicalPlanSource] = {
    if (DefaultSource.relations.contains(tableName)) {
      Some(BaseRelationSource(
        new DummyRelationWithTempFlag(sqlContext, DefaultSource.standardSchema, false)))
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

  override def getRelation(sqlContext: SQLContext, name: Seq[String], options: Map[String, String])
    : Option[RelationInfo] = {
    DefaultSource.relations.find(r => r.equals(name.last))
      .map(r => RelationInfo(r, isTemporary = false, "TABLE", Some("<DDL statement>")))
  }

  override def getRelations(sqlContext: SQLContext, options: Map[String, String])
    : Seq[RelationInfo] = {
    DefaultSource.relations.map(r =>
      RelationInfo(r, isTemporary = false, "TABLE", Some("<DDL statement>")))
  }

  override def getTableNames(sqlContext: SQLContext, parameters: Map[String, String])
    : Seq[String] = {
    DefaultSource.relations
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
