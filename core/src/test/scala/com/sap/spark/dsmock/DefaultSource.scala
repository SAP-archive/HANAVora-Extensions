package com.sap.spark.dsmock

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.RawDDLObjectType.RawDDLObjectType
import org.apache.spark.sql.sources.RawDDLStatementType.RawDDLStatementType
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{PersistedDimensionView, PersistedView}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataType, StructType}
import org.mockito.Mockito._

import scala.collection.mutable

/**
  * A mockable data source for spark.
  *
  * Caution: Before instantiating this, you need to call
  * `DefaultSource.withMock`. This will bind an instance of
  * a mockable default source to your current thread.
  * Apply [[org.mockito.Mockito]] `when` calls to the underlying field.
  */
class DefaultSource
  extends TemporaryAndPersistentSchemaRelationProvider
  with TemporaryAndPersistentRelationProvider
  with PartitionedRelationProvider
  with RegisterAllTableRelations
  with ViewProvider
  with DimensionViewProvider
  with DatasourceCatalog
  with PartitioningFunctionProvider
  with RawSqlSourceProvider
  with MetadataCatalog {

  val underlying: DefaultSource = DefaultSource.currentMock

  override def getSchemas(sqlContext: SQLContext,
                          options: Map[String, String]): Map[RelationKey, SchemaDescription] =
    underlying.getSchemas(sqlContext, options)

  override def getTableMetadata(sqlContext: SQLContext,
                                options: Map[String, String]): Seq[TableMetadata] =
    underlying.getTableMetadata(sqlContext, options)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    underlying.createRelation(sqlContext, parameters)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    underlying.createRelation(sqlContext, parameters, schema)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              schema: StructType,
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    underlying.createRelation(sqlContext, tableName, parameters, schema, isTemporary, allowExisting)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    underlying.createRelation(sqlContext, tableName, parameters, isTemporary, allowExisting)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation  =
    underlying.createRelation(
      sqlContext,
      tableName,
      parameters,
      partitioningFunction,
      partitioningColumns,
      isTemporary,
      allowExisting)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              schema: StructType,
                              partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    underlying.createRelation(
      sqlContext,
      tableName,
      parameters,
      schema,
      partitioningFunction,
      partitioningColumns,
      isTemporary,
      allowExisting)

  override def getTableRelation(tableName: String,
                                sqlContext: SQLContext,
                                options: Map[String, String]): Option[LogicalPlanSource] =
    underlying.getTableRelation(tableName, sqlContext, options)

  override def getAllTableRelations(sqlContext: SQLContext,
                                    options: Map[String, String])
    : Map[String, LogicalPlanSource] =
    underlying.getAllTableRelations(sqlContext, options)

  override def dropView(dropViewInput: DropViewInput): Unit =
    underlying.dropView(dropViewInput)

  override def createView(createViewInput: CreateViewInput): ViewHandle =
    underlying.createView(createViewInput)

  override def createDimensionView(createViewInput: CreateViewInput): ViewHandle =
    underlying.createDimensionView(createViewInput)

  override def dropDimensionView(dropViewInput: DropViewInput): Unit =
    underlying.dropDimensionView(dropViewInput)

  override def getRelation(sqlContext: SQLContext,
                           name: Seq[String],
                           options: Map[String, String]): Option[RelationInfo] = {
    underlying.getRelation(sqlContext, name, options).map { info =>
      RelationInfo(info.name, info.isTemporary, info.kind, info.ddl)
    }
  }

  override def getRelations(sqlContext: SQLContext,
                            options: Map[String, String]): Seq[RelationInfo] =
    underlying.getRelations(sqlContext, options).map { info =>
      RelationInfo(info.name, info.isTemporary, info.kind, info.ddl)
    }

  override def getTableNames(sqlContext: SQLContext,
                             parameters: Map[String, String]): Seq[String] =
    underlying.getTableNames(sqlContext, parameters)

  override def dropPartitioningFunction(sqlContext: SQLContext,
                                        parameters: Map[String, String],
                                        name: String,
                                        allowNotExisting: Boolean): Unit =
    underlying.dropPartitioningFunction(sqlContext, parameters, name, allowNotExisting)

  override def createRangeSplitPartitioningFunction(sqlContext: SQLContext,
                                                    parameters: Map[String, String],
                                                    name: String,
                                                    datatype: DataType,
                                                    splitters: Seq[Int],
                                                    rightClosed: Boolean): Unit =
    underlying.createRangeSplitPartitioningFunction(
      sqlContext,
      parameters,
      name,
      datatype,
      splitters,
      rightClosed)

  override def createHashPartitioningFunction(sqlContext: SQLContext,
                                              parameters: Map[String, String],
                                              name: String,
                                              datatypes: Seq[DataType],
                                              partitionsNo: Option[Int]): Unit =
    underlying.createHashPartitioningFunction(
      sqlContext,
      parameters,
      name,
      datatypes,
      partitionsNo)

  override def createRangeIntervalPartitioningFunction(sqlContext: SQLContext,
                                                       parameters: Map[String, String],
                                                       name: String,
                                                       datatype: DataType,
                                                       start: Int,
                                                       end: Int,
                                                       strideParts: Either[Int, Int]): Unit =
    underlying.createRangeIntervalPartitioningFunction(
      sqlContext,
      parameters,
      name,
      datatype,
      start,
      end,
      strideParts)

  override def getAllPartitioningFunctions(sqlContext: SQLContext,
                                           parameters: Map[String, String])
    : Seq[PartitioningFunction] =
    underlying.getAllPartitioningFunctions(sqlContext, parameters)

  override def getRDD(sqlCommand: String): RDD[Row] = underlying.getRDD(sqlCommand)

  override def getResultingAttributes(sqlCommand: String): Seq[Attribute] =
    underlying.getResultingAttributes(sqlCommand)

  // does nothing
  override def executeDDL(identifier: String,
                          objectType: RawDDLObjectType,
                          statementType: RawDDLStatementType,
                          sparkSchema: Option[StructType],
                          ddlCommand: String,
                          options: Map[String, String]): Unit = {}
}

object DefaultSource {
  private val mocks = new mutable.HashMap[Long, DefaultSource]()

  private def currentMock: DefaultSource = mocks.getOrElse(threadId,
    throw new IllegalAccessException("You need to call `DefaultSource.withMock` " +
      "before instantiating DefaultSource objects"))

  private def threadId: Long = Thread.currentThread().getId

  /**
    * Creates a context for doing operations with a mocked [[DefaultSource]].
    *
    * Mocking calls can be directly done on the function parameter object.
    * All new calls in the created context will return a [[DefaultSource]]
    * with the same mock as underlying attribute.
    *
    * @param op The context of operations
    * @tparam A The return type of the context
    * @return The result of the context operation
    */
  def withMock[A](op: DefaultSource => A): A = {
    val preparedMock = mock(classOf[DefaultSource])
    mocks(threadId) = preparedMock
    val result = op(preparedMock)
    mocks.remove(threadId)
    result
  }
}
