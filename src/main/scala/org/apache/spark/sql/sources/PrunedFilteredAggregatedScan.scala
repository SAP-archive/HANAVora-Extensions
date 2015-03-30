package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._

/**
 * Marks a relation as supporing aggregations push downs.
 */
trait PrunedFilteredAggregatedScan extends BaseRelation {

  def buildScanAggregate(
                          requiredColumns: Array[String],
                          filters: Array[Filter],
                          ge : Seq[Expression],
                          pc : Seq[NamedExpression]): RDD[Row]
}
