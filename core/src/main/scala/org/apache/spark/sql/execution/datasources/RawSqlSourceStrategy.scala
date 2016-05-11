package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{SQLContext, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SelectWith}
import org.apache.spark.sql.execution.{PhysicalRDD, RDDConversions, SparkPlan}
import org.apache.spark.sql.sources.RawSqlSourceProvider

/**
  * Translates [[org.apache.spark.sql.catalyst.plans.logical.SelectWith]] into a physical plan
  */
private[sql] object RawSqlSourceStrategy extends Strategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case SelectWith(sqlCommand, className, attributes) => {
      // 1. instantiate "class name" which is the provider
      val dataSource: Any = ResolvedDataSource.lookupDataSource(className).newInstance()

      // 2. call a method on the newly created object to return an RDD
      dataSource match {
        case rawSql:RawSqlSourceProvider =>
          // 3. get attributes, RDD and convert it to an RDD[InternalRow]
          val rowRDD = rawSql.getRDD(sqlCommand)
          val internalRowRDD = RDDConversions.rowToRowRdd(rowRDD, attributes.map(_.dataType))

          // 4. pack that into Physical RDD
          Seq(PhysicalRDD(attributes, internalRowRDD, s"Raw SQL extraction for ${sqlCommand}"))
      }
    }
    case _ => Nil
  }
}
