package org.apache.spark.sql.catalyst.analysis

import com.sap.spark.PlanTest
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.sources.{BaseRelation, LogicalRelation}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class DropUnneededAliasesSuite extends FunSuite with MockitoSugar with PlanTest {

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

  test("Replace alias into aliases") {
    val avgExpr = avg(ageAtt)
    val avgAlias = avgExpr as 'avgAlias
    val aliasAlias = avgAlias as 'aliasAlias
    val aliasAliasAlias = aliasAlias as 'aliasAliasAlias
    val copiedAlias = Alias(avgExpr, aliasAlias.name)(
      exprId = aliasAlias.exprId
    )
    val copiedAlias2 = Alias(avgExpr, aliasAliasAlias.name)(
      exprId = aliasAliasAlias.exprId
    )

    assertResult(
      lr1.groupBy(avgAlias.toAttribute)(avgAlias)
    )(DropUnneededAliases(lr1.groupBy(avgAlias.toAttribute)(avgAlias)))

    assertResult(
      lr1.groupBy(copiedAlias.toAttribute)(copiedAlias)
    )(DropUnneededAliases(lr1.groupBy(aliasAlias.toAttribute)(aliasAlias)))

    assertResult(
      lr1.groupBy(copiedAlias2.toAttribute)(copiedAlias2)
    )(DropUnneededAliases(lr1.groupBy(aliasAliasAlias.toAttribute)(aliasAliasAlias)))
  }

  test("Replace alias into expressions") {
    val ageAlias = ageAtt as 'ageAlias
    val avgExpr = avg(ageAlias) as 'avgAlias
    val correctedAvgExpr = avg(ageAtt) as 'avgAlias
    comparePlans(
      lr1.groupBy(correctedAvgExpr.toAttribute)(correctedAvgExpr),
      DropUnneededAliases(lr1.groupBy(avgExpr.toAttribute)(avgExpr))
    )
  }

}
