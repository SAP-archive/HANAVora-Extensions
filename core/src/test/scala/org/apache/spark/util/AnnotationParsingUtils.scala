package org.apache.spark.util

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedAlias}
import org.apache.spark.sql.catalyst.expressions._
import org.scalatest.FunSuite

/**
 * Set of utility methods to run tests with annotations
 */
trait AnnotationParsingUtils extends FunSuite {

  def assertAnnotatedAttribute(expectedAliasName:String, expectedAliasChild: Expression,
                               expectedAnnotations: Map[String, Expression],
                               actual: NamedExpression):
  Unit = {
    assert(actual.isInstanceOf[UnresolvedAlias])
    assert(actual.asInstanceOf[UnresolvedAlias].child.isInstanceOf[AnnotatedAttribute])
    val attribute = actual.asInstanceOf[UnresolvedAlias].child.asInstanceOf[AnnotatedAttribute]
    assertResult(expectedAnnotations.keySet)(attribute.annotations.keySet)
    expectedAnnotations.foreach({
      case (k, v:Literal) =>
        assert(v.semanticEquals(attribute.annotations.get(k).get))
    })
    assert(attribute.child.isInstanceOf[Alias])
    val alias = attribute.child.asInstanceOf[Alias]
    assertResult(expectedAliasName)(alias.name)
    assertResult(expectedAliasChild)(alias.child)
  }

  def assertAnnotatedProjection(expected: Seq[(String, UnresolvedAttribute, Map[String, Literal])])
                               (actual: Seq[NamedExpression]): Unit = {
    actual.zip(expected).foreach{case (exp: NamedExpression, values: (
      String, UnresolvedAttribute, Map[String, Expression])) =>
      assertAnnotatedAttribute(values._1, values._2, values._3, exp)}
  }

}
