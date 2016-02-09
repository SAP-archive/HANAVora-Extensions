package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

/** A trait to ease future implementations of table functions. */
trait PhysicalTableFunction extends SparkPlan {
  /** Output schema of the table function. */
  override lazy val schema: StructType = StructType.fromAttributes(output)

  /** Executes the function
    *
    * @return The produced rows.
    */
  protected def run(): Seq[Row]

  /** Executes the run function, then converts them to [[org.apache.spark.rdd.RDD]]s
    *
    * @return The created [[RDD]]s
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val rows = this.run()
    convert(rows)
  }

  /** Converts the given rows to [[org.apache.spark.rdd.RDD]]s using the specified schema.
    *
    * @param rows The rows to convert
    * @return The converted [[RDD]]s
    */
  protected def convert(rows: Seq[Row]): RDD[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
                                          .andThen(_.asInstanceOf[InternalRow])
    val internalRows = rows.map(converter)
    sparkContext.parallelize(internalRows)
  }
}
