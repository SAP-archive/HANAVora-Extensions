package org.apache.spark.sql.sources

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.{PhysicalRDD, RDDConversions, SparkPlan}
import org.apache.spark.sql.sources.RawDDLObjectType.RawDDLObjectType
import org.apache.spark.sql.sources.RawDDLStatementType.RawDDLStatementType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext}

case object RawDDLObjectType {

  sealed trait RawDDLObjectType {
    val name: String
    override def toString: String = name
  }

  sealed abstract class BaseRawDDLObjectType(val name: String) extends RawDDLObjectType
  sealed trait RawData

  case object PartitionFunction extends BaseRawDDLObjectType("partition function")
  case object PartitionScheme   extends BaseRawDDLObjectType("partition scheme")
  case object Collection        extends BaseRawDDLObjectType("collection") with RawData
  case object Series            extends BaseRawDDLObjectType("table") with RawData
  case object Graph             extends BaseRawDDLObjectType("graph") with RawData
}

case object RawDDLStatementType {

  sealed trait RawDDLStatementType

  case object Create extends RawDDLStatementType
  case object Drop   extends RawDDLStatementType
  case object Append extends RawDDLStatementType
  case object Load   extends RawDDLStatementType
}

/**
  * Indicates that a datasource supports the raw sql interface
  */
trait RawSqlSourceProvider {

  /**
    * Returns the spark plan for the execution of the given SQL command.
    *
    * @param sqlContext The Spark [[SQLContext]].
    * @param options The options for the datasource.
    * @param sqlCommand The sql command that to issue.
    * @param expectedOutput The expected output, if any.
    * @return The [[SparkPlan]] executing the given SQL command.
    */
  def executionOf(sqlContext: SQLContext,
                  options: Map[String, String],
                  sqlCommand: String,
                  expectedOutput: Option[StructType]): RawSqlExecution

  /**
    * Runs a DDL statement
    *
    * @param identifier the identifier created by the DDL
    * @param objectType the type of the database object to modify
    * @param statementType the type of the SQL statement to execute
    * @param sparkSchema the type of the columns to be created, optional
    * @param sqlCommand user defined sql string
    * @param options the create options
    */
  def executeDDL(identifier: String,
                 objectType: RawDDLObjectType,
                 statementType: RawDDLStatementType,
                 sparkSchema: Option[StructType],
                 sqlCommand: String,
                 options: Map[String, String]): Unit
}

/** An execution of a SQL statement */
trait RawSqlExecution {

  /** The output schema of the execution */
  def schema: StructType

  /** Creates the spark plan representing this execution */
  def createSparkPlan(): SparkPlan

  /**
    * @return The Spark [[SQLContext]]
    */
  @transient
  def sqlContext: SQLContext

  @transient
  lazy val statistics: Statistics =
    Statistics(sizeInBytes = BigInt(sqlContext.conf.defaultSizeInBytes))

  lazy val output: Seq[Attribute] = schema.toAttributes

  def rowRddToSparkPlan(rowRdd: RDD[Row]): SparkPlan = {
    val internalRdd = RDDConversions.rowToRowRdd(rowRdd, schema.map(_.dataType))
    PhysicalRDD(output, internalRdd, "Raw SQL execution")
  }
}

/** A [[RawSqlExecution]] whose schema initialization is lazy */
trait LazySchemaRawSqlExecution extends RawSqlExecution {
  private val schemaOpt: AtomicReference[Option[StructType]] = new AtomicReference(None)

  /**
    * Retrieves the [[Some]] schema if it was already calculated and [[None]] otherwise.
    *
    * @return [[Some]][[StructType]] if `schema` was already accessed, [[None]] otherwise.
    */
  def schemaOption: Option[StructType] = schemaOpt.get

  /** @inheritdoc */
  override def schema: StructType = synchronized {
    schemaOpt.get.getOrElse {
      val _schema = calculateSchema()
      schemaOpt.set(Some(_schema))
      _schema
    }
  }

  /**
    * Calculates the schema of this execution. This is called at most once.
    *
    * @return The calculated [[StructType]].
    */
  protected def calculateSchema(): StructType
}
