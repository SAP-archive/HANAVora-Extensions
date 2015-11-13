package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation

/**
  * FIXME: Workaround to the lack of visibility of some Spark SQL APIs.
  * E.g.
  * https://issues.apache.org/jira/browse/SPARK-7275
  */
object IsLogicalRelation {

  private lazy val logicalRelationClass =
    org.apache.spark.SPARK_VERSION match {
      case x if x startsWith "1.4" =>
        Class.forName("org.apache.spark.sql.sources.LogicalRelation")
      case x if x startsWith "1.5" =>
        Class.forName("org.apache.spark.sql.execution.datasources.LogicalRelation")
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }

  private lazy val relationMethod =
    logicalRelationClass
      .getMethods
      .find(_.getName == "relation").get

  def unapply(plan: LogicalPlan): Option[BaseRelation] = {
    val planClass = plan.getClass
    if (planClass != logicalRelationClass) {
      None
    } else {
      Some(relationMethod.invoke(plan).asInstanceOf[BaseRelation])
    }
  }

}
