package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.Hierarchy
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.compat._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class ResolveHierarchySuite extends FunSuite with MockitoSugar {

  val br1 = new BaseRelation {
    override def sqlContext: SQLContext = mock[SQLContext]
    override def schema: StructType = StructType(Seq(
      StructField("id", IntegerType),
      StructField("parent", IntegerType)
    ))
  }

  val lr1 = LogicalRelation(br1)
  val idAtt = lr1.output.find(_.name == "id").get
  val parentAtt = lr1.output.find(_.name == "parent").get

  test("Check parenthood expression has no conflicting expression IDs and qualifiers") {
    val source = SimpleAnalyzer.execute(lr1.select('id, 'parent).subquery('u))
    assert(source.resolved)

    val hierarchy = Hierarchy(
      source, "v",
      /* Use the same attributes, so we can check exprId and qualifiers are distinct */
      UnresolvedAttribute("u" :: "id" :: Nil) ===
        UnresolvedAttribute("v" :: "id" :: Nil),
      Nil,
      Some('id.isNull),
      'node
    )

    val resolveHierarchy = ResolveHierarchy(SimpleAnalyzer)
    val resolveReferences = ResolveReferencesWithHierarchies(SimpleAnalyzer)

    val resolvedHierarchy = (0 to 10).foldLeft(hierarchy: Hierarchy) { (h, _) =>
      SimpleAnalyzer.ResolveReferences(
        resolveReferences(resolveHierarchy(h))
      ).asInstanceOf[Hierarchy]
    }

    assert(resolvedHierarchy.nodeAttribute.resolved)
    assert(resolvedHierarchy.parenthoodExpression.resolved)
    assert(resolvedHierarchy.startWhere.map(_.resolved).getOrElse(true))
    assert(resolvedHierarchy.childrenResolved)
    assert(resolvedHierarchy.resolved)

    val parenthoodExpression = resolvedHierarchy
      .asInstanceOf[Hierarchy]
      .parenthoodExpression
      .asInstanceOf[EqualTo]

    assertResult("u" :: Nil)(parenthoodExpression.left.asInstanceOf[Attribute].qualifiers)
    assertResult("v" :: Nil)(parenthoodExpression.right.asInstanceOf[Attribute].qualifiers)
    assert(parenthoodExpression.right.asInstanceOf[Attribute].exprId !=
      source.output.find(_.name == "id").get.exprId)
  }

}
