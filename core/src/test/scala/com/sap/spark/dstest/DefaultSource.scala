package com.sap.spark.dstest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
 * Test default source that is capable of creating dummy temporary and persistent relations
 */
class DefaultSource extends TemporaryAndPersistentSchemaRelationProvider
  with TemporaryAndPersistentRelationProvider
  with PartitionedRelationProvider
  with RegisterAllTableRelations
  with ViewProvider
  with DimensionViewProvider
  with DatasourceCatalog
  with PartitioningFunctionProvider {

  override def createHashPartitioningFunction(sqlContext: SQLContext,
                                              parameters: Map[String, String],
                                              name: String,
                                              datatypes: Seq[DataType],
                                              partitionsNo: Option[Int]): Unit = {
    DefaultSource.addPartitioningFunction(name,
      HashPartitioningFunction(name, datatypes, partitionsNo))
  }

  override def createRangeSplitPartitioningFunction(sqlContext: SQLContext,
                                                    parameters: Map[String, String],
                                                    name: String,
                                                    datatype: DataType,
                                                    splitters: Seq[Int],
                                                    rightClosed: Boolean): Unit = {
    DefaultSource.addPartitioningFunction(name,
      RangeSplitPartitioningFunction(name, datatype, splitters, rightClosed))
  }

  override def createRangeIntervalPartitioningFunction(sqlContext: SQLContext,
                                                       parameters: Map[String, String],
                                                       name: String,
                                                       datatype: DataType,
                                                       start: Int,
                                                       end: Int,
                                                       strideParts: Either[Int, Int]): Unit = {
    DefaultSource.addPartitioningFunction(name,
      RangeIntervalPartitioningFunction(name, datatype, start, end,
        Dissector.fromEither(strideParts)))
  }

  override def getAllPartitioningFunctions(sqlContext: SQLContext,
                                           parameters: Map[String, String])
    : Seq[PartitioningFunction] = {
    DefaultSource.getAllPartitioningFunctions
  }

  override def dropPartitioningFunction(sqlContext: SQLContext,
                                        parameters: Map[String, String],
                                        name: String,
                                        allowNotExisting: Boolean): Unit = {
    DefaultSource.dropPartitioningFunction(name, allowNotExisting)
  }

  // (YH) shouldn't we also add the created table name to the tables sequence in DefaultSource?
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    createRelation(
      sqlContext,
      Seq.empty,
      parameters,
      None,
      None,
      isTemporary = false,
      allowExisting = false)

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    new DummyRelationWithoutTempFlag(sqlContext, schema)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext, tableName, DefaultSource.standardSchema, isTemporary)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              sparkSchema: StructType, isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext, tableName, sparkSchema, isTemporary)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(
      sqlContext,
      tableName,
      DefaultSource.standardSchema,
      isTemporary)

  override def createRelation(sqlContext: SQLContext,
                              tableName: Seq[String],
                              parameters: Map[String, String],
                              sparkSchema: StructType,
                              partitioningFunction: Option[String],
                              partitioningColumns: Option[Seq[String]],
                              isTemporary: Boolean,
                              allowExisting: Boolean): BaseRelation =
    new DummyRelationWithTempFlag(sqlContext, tableName, sparkSchema, isTemporary)

  override def getAllTableRelations(sqlContext: SQLContext,
                                    options: Map[String, String])
    : Map[String, LogicalPlanSource] = {
    DefaultSource.tables.map(name =>
      (name, BaseRelationSource(
        new DummyRelationWithTempFlag(sqlContext, Seq(name), DefaultSource.standardSchema, false)))
    ).toMap ++
    DefaultSource.views.map {
      case (name, (kind, query)) =>
        name -> CreatePersistentViewSource(query, DefaultSource.DropViewHandle(name, kind))
    }
  }


  override def getTableRelation(tableName: String, sqlContext: SQLContext,
                                options: Map[String, String]): Option[LogicalPlanSource] = {
    if (DefaultSource.tables.contains(tableName)) {
      Some(BaseRelationSource(
        new DummyRelationWithTempFlag(
          sqlContext,
          Seq(tableName),
          DefaultSource.standardSchema,
          temporary = false)))
    } else {
      if (DefaultSource.views.contains(tableName)) {
        val (kind, query) = DefaultSource.views(tableName)
        Some(CreatePersistentViewSource(query, DefaultSource.DropViewHandle(tableName, kind)))
      } else {
        None
      }
    }
  }

  override def getRelation(sqlContext: SQLContext, name: Seq[String], options: Map[String, String])
    : Option[RelationInfo] = {
    DefaultSource.tables.find(r => r.equals(name.last))
      .map(r => RelationInfo(r, isTemporary = false, "TABLE", Some("<DDL statement>")))
      .orElse(DefaultSource.views.find(v => v._1 == name.last)
        .map {
          case (viewName, (kind, query)) =>
            RelationInfo(viewName, isTemporary = false, kind, Some(query))
        })
  }

  override def getRelations(sqlContext: SQLContext, options: Map[String, String])
    : Seq[RelationInfo] = {
    DefaultSource.tables.map(r =>
      RelationInfo(r, isTemporary = false, "TABLE", Some("<DDL statement>"))) ++
    DefaultSource.views.map {
      case (name, (kind, query)) => RelationInfo(name, isTemporary = false, kind, Some(query))
    }
  }

  override def getTableNames(sqlContext: SQLContext, parameters: Map[String, String])
    : Seq[String] = {
    DefaultSource.tables
  }

  /**
   * @inheritdoc
   */
  override def createView(input: CreateViewInput): ViewHandle = {
    DefaultSource.addView(input.identifier.table, "view", input.viewSql)
  }

  /**
   * @inheritdoc
   */
  override def dropView(dropViewInput: DropViewInput): Unit = {
    DefaultSource.dropView("view", dropViewInput)
  }

  /**
   * @inheritdoc
   */
  override def createDimensionView(input: CreateViewInput): ViewHandle = {
    DefaultSource.addView(input.identifier.table, "dimension", input.viewSql)
  }

  /**
   * @inheritdoc
   */
  override def dropDimensionView(dropViewInput: DropViewInput): Unit = {
    DefaultSource.dropView("dimension", dropViewInput)
  }
}

/**
 * Companion Object handling already existing relations
  *
  * Contains a relation with the name "StandardTestTable" you might add additional ones. This one
  * is for the thriftserver test.
  *
 */
object DefaultSource {

  private val standardSchema = StructType(Seq(StructField("field", IntegerType, nullable = true)))

  val standardRelation = "StandardTestTable"

  private var tables = Seq(standardRelation)
  private var views = Map.empty[String, (String, String)]

  private val partitioningFunctions = new mutable.HashMap[String, PartitioningFunction]()

  def addRelation(name: String): Unit = {
    tables = tables ++ Seq(name)
  }

  def addView(name: String, kind: String, query: String): ViewHandle = {
    views = views + (name -> (kind, query))
    DropViewHandle(name, kind)
  }

  def dropView(kind: String, dropViewInput: DropViewInput): Unit =
    dropView(kind, dropViewInput.identifier.table)

  def dropView(kind: String, name: String): Unit = {
    views = views.filterNot {
      case (viewName, (viewKind, _)) =>
        viewName == name && viewKind == kind
    }
  }

  def reset(keepStandardRelations: Boolean = true): Unit = {
    if (keepStandardRelations) {
      tables = Seq(standardRelation)
    } else {
      tables = Seq.empty[String]
    }
    views = Map.empty[String, (String, String)]
    partitioningFunctions.clear()
  }

  def addPartitioningFunction(name: String, pFun: PartitioningFunction): Unit = {
    partitioningFunctions(name) = pFun
  }

  def dropPartitioningFunction(name: String, allowNotExisting: Boolean): Unit = {
    if (!partitioningFunctions.contains(name) && !allowNotExisting) {
      throw new RuntimeException(s"Cannot drop non-existent partitioning function $name")
    }
    partitioningFunctions.remove(name)
  }

  def getAllPartitioningFunctions: Seq[PartitioningFunction] =
    partitioningFunctions.values.toSeq

  case class DropViewHandle(viewName: String, viewKind: String) extends ViewHandle {
    override def drop(): Unit = dropView(viewName, viewKind)
  }
}
