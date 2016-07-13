package org.apache.spark.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{ColumnName, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource, LogicalPlanRDD, TableScan}
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.types.{StructField, StructType}

/**
  * Utility functions for creating dummy [[BaseRelation]]s.
  */
object DummyRelationUtils {
  /**
    * Converts a single [[StructField]] to a [[StructType]] with the single field.
    *
    * @param structField The [[StructField]].
    * @return The [[StructType]] with the single [[StructField]].
    */
  implicit def structFieldToStructType(structField: StructField): StructType =
    StructType(structField :: Nil)

  /**
    * Converts a [[Symbol]] to a [[ColumnName]] for easy [[StructField]] construction.
    *
    * @param symbol The [[Symbol]] to convert.
    * @return The [[ColumnName]] with the name of the given [[Symbol]].
    */
  implicit def symbolToColumnName(symbol: Symbol): ColumnName = new ColumnName(symbol.name)

  /**
    * Utility class to avoid conflicts when implicitly converting [[Symbol]]s to [[ColumnName]]s.
    *
    * @param symbol The [[Symbol]] to convert.
    */
  implicit class RichSymbol(symbol: Symbol) {
    /**
      * Returns a [[ColumnName]] with the name of the given [[Symbol]].
      *
      * @return A [[ColumnName]] with the name of the given [[Symbol]].
      */
    def ofType: ColumnName = new ColumnName(symbol.name)
  }

  /**
    * A dummy relation with a schema that implements [[BaseRelation]].
    *
    * @param schema The [[StructType]] schema of the relation.
    * @param sqlContext The Spark [[SQLContext]].
    */
  case class DummyRelation(
      schema: StructType)
     (@transient implicit val sqlContext: SQLContext)
    extends BaseRelation

  /**
    * A dummy relation with a schema and a predefined result for a [[TableScan]].
    *
    * @param schema The [[StructType]] schema of the relation.
    * @param resultRows The [[Seq]] of [[Row]]s resulting from a table scan.
    * @param sqlContext The Spark [[SQLContext]].
    */
  case class DummyRelationWithTableScan(
      schema: StructType,
      resultRows: Seq[Row])
     (@transient implicit val sqlContext: SQLContext)
    extends BaseRelation
    with TableScan {

    override def buildScan(): RDD[Row] = sqlContext.sparkContext.parallelize(resultRows)
  }

  /**
    * A dummy relation that implements [[BaseRelation]] and [[SqlLikeRelation]].
    *
    * @param tableName The table name for the [[SqlLikeRelation]].
    * @param schema The [[StructType]] schema of the relation.
    * @param sqlContext The Spark [[SQLContext]].
    */
  case class SqlLikeDummyRelation(
      tableName: String,
      schema: StructType)
     (@transient implicit val sqlContext: SQLContext)
    extends BaseRelation
    with SqlLikeRelation

  /**
    * A dummy relation that implements [[CatalystSource]] and allows for exchanging its methods.
    *
    * @param schema The [[StructType]] schema of the relation.
    * @param isMultiplePartitionExecutionFunc The function to be executed to determine if this is
    *                                         a multiple partition execution. Defaults to `true`.
    * @param supportsLogicalPlanFunc The function to be executed to determine if this supports a
    *                                given [[LogicalPlan]]. Defaults to `true`.
    * @param supportsExpressionFunc The function to be executed to determine if this supports a
    *                               given [[Expression]]. Defaults to `true`.
    * @param logicalPlanToRDDFunc The function to convert a given [[LogicalPlan]] into an
    *                             [[RDD]] of [[Row]]s. Defaults to [[LogicalPlanRDD]].
    * @param sqlContext The Spark [[SQLContext]].
    */
  case class DummyCatalystSourceRelation(
      schema: StructType,
      isMultiplePartitionExecutionFunc: Option[Seq[CatalystSource] => Boolean] = None,
      supportsLogicalPlanFunc: Option[LogicalPlan => Boolean] = None,
      supportsExpressionFunc: Option[Expression => Boolean] = None,
      logicalPlanToRDDFunc: Option[LogicalPlan => RDD[Row]] = None)
     (@transient implicit val sqlContext: SQLContext)
    extends BaseRelation
    with CatalystSource {

    override def isMultiplePartitionExecution(relations: Seq[CatalystSource]): Boolean =
      isMultiplePartitionExecutionFunc.forall(_.apply(relations))

    override def supportsLogicalPlan(plan: LogicalPlan): Boolean =
      supportsLogicalPlanFunc.forall(_.apply(plan))

    override def supportsExpression(expr: Expression): Boolean =
      supportsExpressionFunc.forall(_.apply(expr))

    override def logicalPlanToRDD(plan: LogicalPlan): RDD[Row] =
      logicalPlanToRDDFunc.getOrElse(
        (plan: LogicalPlan) => new LogicalPlanRDD(plan, sqlContext.sparkContext)).apply(plan)
  }
}
