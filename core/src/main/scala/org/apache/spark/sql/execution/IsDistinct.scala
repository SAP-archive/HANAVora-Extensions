package org.apache.spark.sql.execution

import org.apache.spark.sql.execution

/**
  * Parts of Distinct operations are transalted to Aggregates. Therefor this object is added
  * to make it easier to translate a given plan to an aggregate. Look into calling context
  * for more information.
  */
object IsDistinct {
  def apply(child: SparkPlan): SparkPlan =
    execution.Aggregate(partial = false, child.output, child.output, child)
}
