package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.expressions._

object SqlLikeRelationSuiteHelper extends SqlLikeRelation {

override def tableName: String = "table"

/* Wrapper to call the protected function */
override def prepareAggregationsUsingAliases(aggregationExpressions: Seq[NamedExpression],
                                             groupingExpressions: Seq[Expression]):
  (Seq[NamedExpression], Seq[Expression]) =
    super.prepareAggregationsUsingAliases(aggregationExpressions, groupingExpressions)
}
