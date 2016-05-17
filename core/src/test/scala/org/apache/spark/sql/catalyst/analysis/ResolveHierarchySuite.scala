package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo}
import org.apache.spark.sql.catalyst.plans.logical.{AdjacencyListHierarchySpec, Hierarchy}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
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
      AdjacencyListHierarchySpec(source, "v",
        /* Use the same attributes, so we can check exprId and qualifiers are distinct */
        UnresolvedAttribute("u" :: "id" :: Nil) === UnresolvedAttribute("v" :: "id" :: Nil),
        Some('id.isNull), Nil),
      'node
    )

    val resolveHierarchy = ResolveHierarchy(SimpleAnalyzer)
    val resolveReferences = ResolveReferencesWithHierarchies(SimpleAnalyzer)

    val resolvedHierarchy = (0 to 10).foldLeft(hierarchy: Hierarchy) { (h, _) =>
      SimpleAnalyzer.ResolveReferences(
        resolveReferences(resolveHierarchy(h))
      ).asInstanceOf[Hierarchy]
    }

    assert(resolvedHierarchy.node.resolved)
    val resolvedSpec = resolvedHierarchy.spec.asInstanceOf[AdjacencyListHierarchySpec]
    assert(resolvedSpec.parenthoodExp.resolved)
    assert(resolvedSpec.startWhere.forall(_.resolved))
    assert(resolvedHierarchy.childrenResolved)
    assert(resolvedHierarchy.resolved)

    val parenthoodExpression = resolvedSpec.parenthoodExp.asInstanceOf[EqualTo]

    assertResult("u" :: Nil)(parenthoodExpression.left.asInstanceOf[Attribute].qualifiers)
    assertResult("v" :: Nil)(parenthoodExpression.right.asInstanceOf[Attribute].qualifiers)
    assert(parenthoodExpression.right.asInstanceOf[Attribute].exprId !=
      source.output.find(_.name == "id").get.exprId)
  }

}
