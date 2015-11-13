package org.apache.spark.sql

import com.sap.spark.PlanTest
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.analysis.compat._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.CreateViewCommand
import org.apache.spark.sql.types.compat._
import org.scalatest.FunSuite

class SapSqlParserSuite extends FunSuite with PlanTest with Logging {

  def t1: LogicalPlan = new LocalRelation(output = Seq(
    new AttributeReference("pred", StringType, nullable = true, metadata = Metadata.empty)(),
    new AttributeReference("succ", StringType, nullable = false, metadata = Metadata.empty)(),
    new AttributeReference("ord", StringType, nullable = false, metadata = Metadata.empty)()
  ).map(_.toAttribute)
  )

  def catalog: Catalog = {
    val catalog = new SimpleCatalog(SimpleCatalystConf(true))
    catalog.registerTable(Seq("T1"), t1)
    catalog
  }

  def analyzer: Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, SimpleCatalystConf(true))
  test("basic case") {
    val parser = new SapParserDialect
    val result = parser.parse(
      """
        |SELECT 1 FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY ord
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)

    val expected = Project(unresolvedAliases(Literal(1)), Subquery("H", Hierarchy(
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

  test("search by with multiple search by expressions") {
    val parser = new SapParserDialect
    val result = parser.parse(
      """
        |SELECT 1 FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY myAttr ASC, otherAttr DESC, yetAnotherAttr
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)
    val expected = Project(unresolvedAliases(Literal(1)), Subquery("H", Hierarchy(
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("myAttr"), Ascending) ::
        SortOrder(UnresolvedAttribute("otherAttr"), Descending) ::
        SortOrder(UnresolvedAttribute("yetAnotherAttr"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    comparePlans(expected, result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

    test("search by with no search by") {
      val parser = new SapParserDialect
      val result = parser.parse(
        """
          |SELECT 1 FROM HIERARCHY (
          | USING T1 AS v
          | JOIN PARENT u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          |) AS H
        """.stripMargin)
      val expected = Project(unresolvedAliases(Literal(1)), Subquery("H", Hierarchy(
        relation = UnresolvedRelation("T1" :: Nil, Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      )))
      comparePlans(expected, result)

      val analyzed = analyzer.execute(result)
      log.info(s"$analyzed")
  }

  test("create view") {
    val parser = new SapParserDialect
    val result = parser.parse("CREATE VIEW myview AS SELECT 1 FROM mytable")
    val expected = CreateViewCommand("myview",
      Project(unresolvedAliases(Literal(1)), UnresolvedRelation("mytable" :: Nil))
    )
    comparePlans(expected, result)
  }

  test("create view of a hierarchy") {
    val parser = new SapParserDialect
    val result = parser.parse("""
                              CREATE VIEW HV AS SELECT 1 FROM HIERARCHY (
                                 USING T1 AS v
                                 JOIN PARENT u ON v.pred = u.succ
                                 START WHERE pred IS NULL
                                 SET Node
                                ) AS H
                              """.stripMargin)
    val expected = CreateViewCommand("HV",
      Project(unresolvedAliases(Literal(1)), Subquery("H", Hierarchy(
        relation = UnresolvedRelation("T1" :: Nil, Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      ))))
    comparePlans(expected, result)
  }

}
