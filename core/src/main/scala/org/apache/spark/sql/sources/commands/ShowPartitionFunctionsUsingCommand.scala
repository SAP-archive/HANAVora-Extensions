package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

case class ShowPartitionFunctionsUsingCommand(
    provider: String,
    options: Map[String, String])
  extends RunnableCommand {

  def run(sqlContext: SQLContext): Seq[Row] = {
    val resolver = DatasourceResolver.resolverFor(sqlContext)
    val pFunProvider = resolver.newInstanceOfTyped[PartitioningFunctionProvider](provider)
    val pFuns = pFunProvider.getAllPartitioningFunctions(sqlContext, options)

    pFuns.map { fun =>
      val (splittersOpt, rightClosedOpt) = fun match {
        case RangeSplitPartitioningFunction(_, _, splitters, rightClosed) =>
          (Some(splitters), Some(rightClosed))
        case _ =>
          (None, None)
      }
      val (startOpt, endOpt, intervalTypeOpt, intervalValueOpt) = fun match {
        case RangeIntervalPartitioningFunction(_, _, start, end, strideParts) =>
          (Some(start), Some(end), Some(strideParts.productPrefix), Some(strideParts.n))
        case _ =>
          (None, None, None, None)
      }
      val partitionsNoOpt = fun match {
        case HashPartitioningFunction(_, _, partitionsNo) =>
          partitionsNo
        case s: SimpleDataType =>
          None
      }
      Row(fun.name, fun.productPrefix, fun.dataTypes.map(_.toString).mkString(","),
        splittersOpt.map(_.mkString(",")).orNull, rightClosedOpt.orNull, startOpt.orNull,
        endOpt.orNull, intervalTypeOpt.orNull, intervalValueOpt.orNull, partitionsNoOpt.orNull)
    }
  }

  override lazy val output: Seq[Attribute] = StructType(
    StructField("name", StringType, nullable = false) ::
      StructField("kind", StringType, nullable = false) ::
      StructField("dataTypes", StringType, nullable = false) ::
      StructField("splitters", StringType, nullable = true) ::
      StructField("rightClosed", BooleanType, nullable = true) ::
      StructField("start", IntegerType, nullable = true) ::
      StructField("end", IntegerType, nullable = true) ::
      StructField("intervalType", StringType, nullable = true) ::
      StructField("intervalValue", IntegerType, nullable = true) ::
      StructField("partitionsNo", IntegerType, nullable = true) :: Nil
  ).toAttributes
}
