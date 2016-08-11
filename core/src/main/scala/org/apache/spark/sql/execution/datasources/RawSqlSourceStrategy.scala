package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SelectUsing}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{SQLContext, Strategy}

/**
  * Translates [[SelectUsing]] into a spark plan given by the current raw sql execution.
  */
private[sql] object RawSqlSourceStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SelectUsing(execution) =>
      execution.createSparkPlan() :: Nil

    case _ => Nil
  }
}
