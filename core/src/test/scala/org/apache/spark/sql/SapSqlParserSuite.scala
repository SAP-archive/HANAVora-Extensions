package org.apache.spark.sql

import com.sap.spark.PlanTest
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.CreateViewCommand
import org.apache.spark.sql.types.{Metadata, StringType}
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
    val parser = new SapSqlParser
    val result = parser.parse(
      """
        |SELECT * FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY ord
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)

    val expected = Project(UnresolvedStar(None) :: Nil, Subquery("H", Hierarchy(
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    assertResult(expected)(result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

  test("search by with multiple search by expressions") {
    val parser = new SapSqlParser
    val result = parser.parse(
      """
        |SELECT * FROM HIERARCHY (
        | USING T1 AS v
        | JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY myAttr ASC, otherAttr DESC, yetAnotherAttr
        | START WHERE pred IS NULL
        | SET Node
        |) AS H
      """.stripMargin)
    val expected = Project(UnresolvedStar(None) :: Nil, Subquery("H", Hierarchy(
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
      searchBy = SortOrder(UnresolvedAttribute("myAttr"), Ascending) ::
        SortOrder(UnresolvedAttribute("otherAttr"), Descending) ::
        SortOrder(UnresolvedAttribute("yetAnotherAttr"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    )))
    assertResult(expected)(result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

    test("search by with no search by") {
      val parser = new SapSqlParser
      val result = parser.parse(
        """
          |SELECT * FROM HIERARCHY (
          | USING T1 AS v
          | JOIN PARENT u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          |) AS H
        """.stripMargin)
      val expected = Project(UnresolvedStar(None) :: Nil, Subquery("H", Hierarchy(
        relation = UnresolvedRelation("T1" :: Nil, Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      )))
      assertResult(expected)(result)

      val analyzed = analyzer.execute(result)
      log.info(s"$analyzed")
  }

  test("create view") {
    val parser = new SapSqlParser
    val result = parser.parse("CREATE VIEW myview AS SELECT * FROM mytable")
    val expected = CreateViewCommand("myview",
      Project(UnresolvedStar(None) :: Nil, UnresolvedRelation("mytable" :: Nil))
    )
    assertResult(expected)(result)
  }

  test("create view of a hierarchy") {
    val parser = new SapSqlParser
    val result = parser.parse("""
                              CREATE VIEW HV AS SELECT * FROM HIERARCHY (
                                 USING T1 AS v
                                 JOIN PARENT u ON v.pred = u.succ
                                 START WHERE pred IS NULL
                                 SET Node
                                ) AS H
                              """.stripMargin)
    val expected = CreateViewCommand("HV",
      Project(UnresolvedStar(None) :: Nil, Subquery("H", Hierarchy(
        relation = UnresolvedRelation("T1" :: Nil, Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = Some(IsNull(UnresolvedAttribute("pred"))),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      ))))
    assertResult(expected)(result)
  }

  test("parse IF statement") {
    val sapParser = new SapSqlParser

    val sql = "SELECT IF(column > 1, 1, NULL) FROM T1"

    val result = sapParser.parse(sql)
    val expected = Project(
      Alias(
        FixedIf(
          GreaterThan(
            UnresolvedAttribute("column"),
            Literal(1)
          ),
          Literal(1),
          Literal(null)
        ), "c0"
      )() :: Nil,
      UnresolvedRelation("T1" :: Nil, None)
    )

    comparePlans(expected, result)
  }
}
