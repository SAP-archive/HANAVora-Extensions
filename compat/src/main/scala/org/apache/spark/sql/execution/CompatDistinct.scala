package org.apache.spark.sql.execution

import org.apache.spark.sql.execution

object CompatDistinct {

  // scalastyle:off cyclomatic.complexity
  def apply(child: SparkPlan): SparkPlan =
    execution.Aggregate(partial = false, child.output, child.output, child)

}
