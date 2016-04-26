package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * A system table.
  */
trait SystemTable extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq.empty

  /**
    * Executes the functionality of this table with the given [[SQLContext]].
    *
    * @param sqlContext The current [[SQLContext]]
    * @return The result of this system table.
    */
  def execute(sqlContext: SQLContext): Seq[Row]
}

