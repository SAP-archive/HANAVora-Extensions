package org.apache.spark.sql.sources

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.Implicits._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{expressions => expr}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Strategy, execution => exec}

/**
 * Strategy to push down functions to a DataSource when they are supported.
 */
private[sql] object PushDownFunctionsStrategy extends Strategy with Logging {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(
    projectExpressions,
    filterExpressions,
    l@LogicalRelation(t: PrunedFilteredExpressionsScan)) => {

      /* Splitting the data depending on data source support */
      val (filtersSupported, filtersNotSupported) =
        filterExpressions.partition(t.supports)
      val (projectsSupported, projectsNotSupported) =
        projectExpressions.partition(t.supports)

      /* Get the attributes of the expressions not supported by the datasource */
      val extractedAttributes = (projectsNotSupported.flatMap(_.extractAttributes) ++
        filtersNotSupported.flatMap(_.extractAttributes)).distinct

      /* Get the unsupported projections and the references of the supported projections */
      val fixedProjection = projectExpressions.map {
        case e if projectsNotSupported.contains(e) => e
        case e => e.toAttribute
      }

      /* Generates the PysicalRDD plan */
      val projectionsAndAttributes = (projectsSupported ++ extractedAttributes).distinct
      val rdd = exec.PhysicalRDD(
        projectionsAndAttributes.map(_.toAttribute),
        t.buildScanWithExpressions(projectionsAndAttributes, filtersSupported))

      /* Generates the new SparkPlan taking care about the supported functionality in each case */
      if (filtersNotSupported.nonEmpty){
        exec.Project(fixedProjection, exec.Filter(
          filtersNotSupported.reduce(expr.And), rdd)) :: Nil
      } else if (projectsNotSupported.nonEmpty){
        exec.Project(fixedProjection, rdd) :: Nil
      }else if (filtersNotSupported.isEmpty && projectsNotSupported.isEmpty){
        rdd :: Nil
      } else {
        Nil
      }
    }
    case _ => Nil
  }

}
