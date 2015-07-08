package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.sources.{BaseRelation, LogicalRelation}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class UseAliasesForAggregationsInGroupingsSuite extends FunSuite with MockitoSugar {

  val br1 = new BaseRelation {
    override def sqlContext: SQLContext = mock[SQLContext]
    override def schema: StructType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
  }

  val lr1 = LogicalRelation(br1)
  val nameAtt = lr1.output.find(_.name == "name").get
  val ageAtt = lr1.output.find(_.name == "age").get

  test("replace functions in group by") {
    val avgExpr = avg(ageAtt)
    val avgAlias = avgExpr as 'avgAlias
    assertResult(
      lr1.groupBy(avgAlias.toAttribute)(avgAlias)
    )(UseAliasesForFunctionsInGroupings(
      lr1.groupBy(avgExpr)(avgAlias))
    )
    assertResult(
      lr1.select(ageAtt)
    )(UseAliasesForFunctionsInGroupings(
      lr1.select(ageAtt))
      )
    intercept[RuntimeException](
      UseAliasesForFunctionsInGroupings(Aggregate(Seq(avgExpr), Seq(ageAtt), lr1))
    )
  }

}
