package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * A system table.
  */
trait SystemTable extends BaseRelation with PrunedFilteredScan {

  /**
    * A handle to the SQL Context that was used to create this plan. Since many operators need
    * access to the sqlContext for RDD operations or configuration this field is automatically
    * populated by the query planning infrastructure.
    */
  @transient
  val sqlContext: SQLContext

  def sparkContext: SparkContext = sqlContext.sparkContext
}

/**
  * A [[SystemTable]] that is capable of filtering and scanning by itself.
  */
trait AutoScan extends ScanAndFilterImplicits {
  this: SystemTable =>

  /** @inheritdoc */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val values = execute()
    val prunedFilteredScan = schema.buildPrunedFilteredScan(requiredColumns, filters)
    val result = prunedFilteredScan(values)
    sparkContext.parallelize(result)
  }

  /**
    * Executes the functionality of this table.
    *
    * @return The result of this system table.
    */
  def execute(): Seq[Row]
}

