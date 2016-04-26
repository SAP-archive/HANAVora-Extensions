package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.systables.SystemTable
import org.apache.spark.sql.{Row, Strategy}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.extension.ExtendedPlanner

/**
  * Strategy to execute [[SystemTable]]s.
  *
  * @param planner The [[ExtendedPlanner]].
  */
case class SystemTablesStrategy(planner: ExtendedPlanner) extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan flatMap {
    case s: SystemTable =>
      PhysicalSystemTable(s) :: Nil
    case _ => Nil
  }
}

/**
  * The physical plan of a [[SystemTable]].
  *
  * @param systemTable The [[SystemTable]] to execute.
  */
case class PhysicalSystemTable(systemTable: SystemTable) extends SparkPlan {
 protected def doExecute(): RDD[InternalRow] = {
    val convert = CatalystTypeConverters.createToCatalystConverter(schema)
    val converted = systemTable.execute(sqlContext).map(convert(_).asInstanceOf[InternalRow])
    sqlContext.sparkContext.parallelize(converted, 1)
  }

  override def output: Seq[Attribute] = systemTable.output

  override def children: Seq[SparkPlan] = Seq.empty
}
