package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType

/** A trait to ease future implementations of table functions. */
trait PhysicalTableFunction extends SparkPlan {
  /** Output schema of the table function. */
  override lazy val schema: StructType = StructType.fromAttributes(output)

  /** Executes the function
    *
    * @param sqlContext The Spark [[SQLContext]].
    * @return The produced rows.
    */
  protected def run(sqlContext: SQLContext): Seq[Seq[Any]]

  /** Executes the run function, then converts them to [[org.apache.spark.rdd.RDD]]s
    *
    * @return The created [[RDD]]s
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val values = this.run(sqlContext)

    // This step makes sure that values corresponding to the schema are returned.
    val rows = values.map(Row.fromSeq)
    val converted = convert(rows)
    sparkContext.parallelize(converted)
  }

  /** Converts the given rows to [[org.apache.spark.rdd.RDD]]s using the specified schema.
    *
    * @param rows The rows to convert
    * @return The converted [[RDD]]s
    */
  protected def convert(rows: Seq[Row]): Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
                                          .andThen(_.asInstanceOf[InternalRow])
    rows.map(converter)
  }
}
