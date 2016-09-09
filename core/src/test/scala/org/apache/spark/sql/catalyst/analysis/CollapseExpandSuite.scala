package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.analysis.CollapseExpandSuite.SqlLikeCatalystSourceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource, Table}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.util.PlanComparisonUtils._
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

/** Suite for testing [[CollapseExpand]] */
class CollapseExpandSuite extends FunSuite with MockitoSugar with GlobalSapSQLContext {
  case object Leaf extends LeafNode {
    override def output: Seq[Attribute] = Seq.empty
  }

  test("Expansion with a single sequence of projections is correctly collapsed") {
    val expand =
      Expand(
        Seq(Seq('a.string, Literal(1))),
        Seq('a.string, 'gid.int),
        Leaf)

    val collapsed = CollapseExpand(expand)
    assertResult(normalizeExprIds(Project(Seq('a.string, Literal(1) as "gid"), Leaf)))(
      normalizeExprIds(collapsed))
  }

  test("Expansion with multiple projections is correctly collapsed") {
    val expand =
      Expand(
        Seq(
          Seq('a.string, Literal(1)),
          Seq('b.string, Literal(1))),
        Seq('a.string, 'gid1.int, 'b.string, 'gid2.int),
        Leaf)

    val collapsed = CollapseExpand(expand)
    assertResult(
      normalizeExprIds(
        Project(Seq(
            'a.string,
            Literal(1) as "gid1",
            'b.string,
            Literal(1) as "gid2"),
          Leaf)))(normalizeExprIds(collapsed))
  }

  test("Expand pushdown integration") {
    val relation = mock[SqlLikeCatalystSourceRelation]
    when(relation.supportsLogicalPlan(any[Expand]))
      .thenReturn(true)
    when(relation.isMultiplePartitionExecution(any[Seq[CatalystSource]]))
      .thenReturn(true)
    when(relation.schema)
      .thenReturn(StructType(StructField("foo", StringType) :: Nil))
    when(relation.relationName)
      .thenReturn("t")
    when(relation.logicalPlanToRDD(any[LogicalPlan]))
      .thenReturn(sc.parallelize(Seq(Row("a", 1), Row("b", 1), Row("a", 1))))

    sqlc.baseRelationToDataFrame(relation).registerTempTable("t")

    val dataFrame = sqlc.sql("SELECT COUNT(DISTINCT foo) FROM t")
    val Seq(Row(ct)) = dataFrame.collect().toSeq

    assertResult(2)(ct)
  }
}

object CollapseExpandSuite {
  abstract class SqlLikeCatalystSourceRelation
    extends BaseRelation
    with Table
    with SqlLikeRelation
    with CatalystSource
}
