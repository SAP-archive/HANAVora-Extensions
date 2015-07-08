package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}

/**
 * Marks a relation as supporing arbitrary functions in push downs.
 */
trait PrunedFilteredExpressionsScan extends BaseRelation with ExpressionSupport {

  /**
   * Push down expression from the projection and the filter
   *
   * @param projectExpressions Project expressions
   * @param filterExpressions Filter expressions
   * @return an RDD[Row] instance
   */
  def buildScanWithExpressions(projectExpressions: Seq[NamedExpression],
                               filterExpressions: Seq[Expression]): RDD[Row]
}
