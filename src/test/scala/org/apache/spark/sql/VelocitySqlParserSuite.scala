package org.apache.spark.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{Metadata, StringType}
import org.scalatest.FunSuite

class VelocitySqlParserSuite extends FunSuite with Logging {

  def t1 : LogicalPlan = new LocalRelation(output = Seq(
    new AttributeReference("pred", StringType, nullable = true, metadata = Metadata.empty)(),
    new AttributeReference("succ", StringType, nullable = false, metadata = Metadata.empty)(),
    new AttributeReference("ord", StringType, nullable = false, metadata = Metadata.empty)()
  ).map(_.toAttribute)
  )

  def catalog : Catalog = {
    val catalog = new SimpleCatalog(SimpleCatalystConf(true))
    catalog.registerTable(Seq("T1"), t1)
    catalog
  }

  def analyzer : Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, SimpleCatalystConf(true))
  test("basic case") {
    val parser = new VelocitySqlParser
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

    val expected = Project(UnresolvedStar(None) :: Nil, Hierarchy(
      alias = "H",
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = IsNull(UnresolvedAttribute("pred")),
      searchBy = SortOrder(UnresolvedAttribute("ord"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    ))
    assertResult(expected)(result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

  test("search by with multiple search by expressions") {
    val parser = new VelocitySqlParser
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
    val expected = Project(UnresolvedStar(None) :: Nil, Hierarchy(
      alias = "H",
      relation = UnresolvedRelation("T1" :: Nil, Some("v")),
      parenthoodExpression = EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
      childAlias = "u",
      startWhere = IsNull(UnresolvedAttribute("pred")),
      searchBy = SortOrder(UnresolvedAttribute("myAttr"), Ascending) ::
        SortOrder(UnresolvedAttribute("otherAttr"), Descending) ::
        SortOrder(UnresolvedAttribute("yetAnotherAttr"), Ascending) :: Nil,
      nodeAttribute = UnresolvedAttribute("Node")
    ))
    assertResult(expected)(result)

    val analyzed = analyzer.execute(result)
    log.info(s"$analyzed")
  }

    test("search by with no search by") {
      val parser = new VelocitySqlParser
      val result = parser.parse(
        """
          |SELECT * FROM HIERARCHY (
          | USING T1 AS v
          | JOIN PARENT u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          |) AS H
        """.stripMargin)
      val expected = Project(UnresolvedStar(None) :: Nil, Hierarchy(
        alias = "H",
        relation = UnresolvedRelation("T1" :: Nil, Some("v")),
        parenthoodExpression =
          EqualTo(UnresolvedAttribute("v.pred"), UnresolvedAttribute("u.succ")),
        childAlias = "u",
        startWhere = IsNull(UnresolvedAttribute("pred")),
        searchBy = Nil,
        nodeAttribute = UnresolvedAttribute("Node")
      ))
      assertResult(expected)(result)

      val analyzed = analyzer.execute(result)
      log.info(s"$analyzed")
  }

}
