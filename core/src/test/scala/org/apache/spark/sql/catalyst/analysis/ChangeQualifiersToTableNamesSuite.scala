package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{SimpleCatalystConf, TableIdentifier}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.util.DummyRelationUtils.SqlLikeDummyRelation
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class ChangeQualifiersToTableNamesSuite extends FunSuite with MockitoSugar {
  val sqlc = mock[SQLContext]

  val schema =
    StructType(Seq(StructField("name", StringType), StructField("age", IntegerType)))

  val br1 =
    SqlLikeDummyRelation("testTable1", schema)(sqlc)

  val br2 =
    SqlLikeDummyRelation("testTable2", schema)(sqlc)

  def t1: LogicalPlan = new LocalRelation(output = Seq(
    new AttributeReference("pred", StringType, nullable = true, metadata = Metadata.empty)(),
    new AttributeReference("succ", StringType, nullable = false, metadata = Metadata.empty)(),
    new AttributeReference("ord", StringType, nullable = false, metadata = Metadata.empty)()
  ).map(_.toAttribute)
  )

  def catalog: Catalog = {
    val catalog = new SimpleCatalog(SimpleCatalystConf(true))
    catalog.registerTable(TableIdentifier("T1"), t1)
    catalog
  }

  def analyzer: Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, SimpleCatalystConf(true))

  val lr1 = LogicalRelation(br1)
  val lr2 = LogicalRelation(br2)
  val nameAtt = lr1.output.find(_.name == "name").get
  val ageAtt = lr1.output.find(_.name == "age").get

  val nameAtt2 = lr2.output.find(_.name == "name").get
  val ageAtt2 = lr2.output.find(_.name == "age").get

  val h = Hierarchy(
    AdjacencyListHierarchySpec(lr1, "u", 'pred==='succ,
      Some(new AttributeReference("blah", StringType, nullable = true,
        metadata = Metadata.empty)().expr),SortOrder('name, Ascending) :: Nil),
    new AttributeReference("bleh", StringType, nullable = true, metadata = Metadata.empty)())

  val aliasedSum1 = sum(nameAtt).as('aliasedSum1)
  val aliasedSum2 = sum(nameAtt).as('aliasedSum2)
  val aliasedSum3 = sum(nameAtt).as('aliasedSum3)

  test("Non-existent attribute reference does not break the rule") {
    val att = AttributeReference("blah", StringType)()
    val input = lr1.subquery('table1).select(att)
    assertResult(Nil)(ChangeQualifiersToTableNames(input).output(0).qualifiers)
  }

  test("Fail with two qualifiers") {
    val nameAttrModified = nameAtt
      .copy()(exprId = nameAtt.exprId, qualifiers = "blah" :: "bleh" :: Nil)
    intercept[RuntimeException] {
      ChangeQualifiersToTableNames(lr1.select(nameAttrModified))
    }
  }

  test("Fix qualifiers") {
    val input = lr1.subquery('table1)
      .select(nameAtt.copy()(exprId = nameAtt.exprId, qualifiers = "Boo" :: Nil))
    assertResult("table1" :: Nil)(ChangeQualifiersToTableNames(input).output(0).qualifiers)
  }

  test("Fix qualifiers with multiple subqueries") {
    val input = lr1
      .select(nameAtt.copy()(exprId = nameAtt.exprId, qualifiers = "Boo" :: Nil))
      .subquery('q)
      .select(nameAtt.copy()(exprId = nameAtt.exprId, qualifiers = "Boo" :: Nil))
    assertResult("q" :: Nil)(ChangeQualifiersToTableNames(input).output(0).qualifiers)
  }

  test("Fix aggregate aliases") {
    // Put subquery between two consecutive aggregates
    assertResult("subquery2" :: Nil) {
      ChangeQualifiersToTableNames(
        lr1.subquery('table1)
          .select(nameAtt).subquery('subquery2)
          .groupBy(aliasedSum1)(nameAtt)).output(0).qualifiers
    }
  }

  test("Hierarchy aliases handling") {
    val result =  ChangeQualifiersToTableNames(
      Hierarchy(
        AdjacencyListHierarchySpec(source = lr1.subquery('u),
          childAlias = "v",
          parenthoodExp = nameAtt === AttributeReference("pred", StringType)(),
          orderBy = Nil,
          startWhere = Some(nameAtt.isNull)),
        node = 'node
      )).asInstanceOf[Hierarchy]
    assertResult("u" :: Nil)(
      result.spec.asInstanceOf[AdjacencyListHierarchySpec].parenthoodExp
      .children.head.asInstanceOf[AttributeReference].qualifiers)
    assertResult("v" :: Nil)(
      result.spec.asInstanceOf[AdjacencyListHierarchySpec].parenthoodExp
        .children(1).asInstanceOf[AttributeReference].qualifiers)
  }

  test("Regression test: Bug 90478") {
    val input = lr1.where(nameAtt === "STRING").select(nameAtt).join(lr2)
    ChangeQualifiersToTableNames(input)
  }

}
