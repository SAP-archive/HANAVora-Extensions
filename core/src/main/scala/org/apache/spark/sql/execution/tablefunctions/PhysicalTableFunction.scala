package org.apache.spark.sql.execution.tablefunctions

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
  protected def run(): Seq[Seq[Any]]

  /** Executes the run function, then converts them to [[org.apache.spark.rdd.RDD]]s
    *
    * @return The created [[RDD]]s
    */
  override protected def doExecute(): RDD[InternalRow] = {
    val values = this.run()
    val enforcer = new SchemaEnforcer(output)

    // This step makes sure that values corresponding to the schema are returned.
    val enforced = enforcer.enforce(values)
    val converted = convert(enforced)
    sparkContext.parallelize(converted)
  }

  /** Converts the given rows to [[org.apache.spark.rdd.RDD]]s using the specified schema.
    *
    * @param values The values to convert
    * @return The converted [[RDD]]s
    */
  protected def convert(values: Seq[Seq[Any]]): Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
                                          .andThen(_.asInstanceOf[InternalRow])
    values.map(value => converter(Row(value:_*)))
  }
}
