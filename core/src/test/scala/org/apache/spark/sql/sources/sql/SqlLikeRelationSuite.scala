package org.apache.spark.sql.sources.sql

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class SqlLikeRelationSuite extends FunSuite with Logging {


  test("Prepare single attribute aggregation and single attribute group expressions") {
    /* Data preparation */
    val attr1 = AttributeReference("attr1", StringType, nullable = false)()
    val attr2 = AttributeReference("attr2", StringType, nullable = false)()

    /* Execution */
    val (preparedAgg, preparedGroup) = Test.prepareAggregationsUsingAliases(Seq(attr1), Seq(attr2))

    /* Expected data preparation */
    val expAttr1 = attr1.copy(attr1.name, attr1.dataType, nullable = true,
      metadata = Metadata.empty)(exprId = null, qualifiers = Nil)
    val expAttr2 = attr2.copy(attr2.name, attr2.dataType, nullable = true,
      metadata = Metadata.empty)(exprId = null, qualifiers = Nil)

    /* Assertions */
    assertResult(expAttr1)(preparedAgg(0))
    assertResult(expAttr2)(preparedGroup(0))
  }

  test("Prepare single alias aggregation and group expressions") {
    /* Data preparation */
    val attr1 = AttributeReference("attr1", StringType, nullable = false)()
    val alias1 = Alias(attr1, "alias1")()

    /* Execution */
    val (preparedAgg, preparedGroup) = Test.prepareAggregationsUsingAliases(Seq(alias1),
      Seq(alias1.toAttribute))

    /* Expected data preparation */
    val expAlias1 = alias1.copy(alias1.child, alias1.name + "EID" + alias1.exprId.id)(alias1.exprId,
      alias1.qualifiers, alias1.explicitMetadata)
    val expAliasAttr1 = AttributeReference(alias1.name, alias1.dataType)(exprId = null)

    /* Assertions */
    assertResult(expAlias1)(preparedAgg(0))
    assertResult(expAliasAttr1)(preparedGroup(0))
  }

  test("Prepare single alias aggregation and single attribute group expressions") {
    /* Data preparation */
    val attr1 = AttributeReference("attr1", StringType, nullable = false)()
    val alias1 = Alias(attr1, "alias1")()

    /* Execution */
    val (preparedAgg, preparedGroup) = Test.prepareAggregationsUsingAliases(Seq(alias1),
      Seq(attr1))

    /* Expected data preparation */
    val expAlias1 = alias1.copy(alias1.child, alias1.name + "EID" + alias1.exprId.id)(alias1.exprId,
      alias1.qualifiers, alias1.explicitMetadata)
    val expAttr1 = AttributeReference(attr1.name, attr1.dataType)(exprId = null)

    /* Assertions */
    assertResult(expAlias1)(preparedAgg(0))
    assertResult(expAttr1)(preparedGroup(0))
  }

  test("Prepare SUM aggregation and single attribute group expressions") {
    /* Data preparation */
    val attr1 = AttributeReference("attr1", IntegerType, nullable = false)()
    val sumAttr1 = Sum(attr1)
    val aliasSumAttr1 = Alias(attr1, "SumAlias")()

    /* Execution */
    val (preparedAgg, preparedGroup) = Test.prepareAggregationsUsingAliases(Seq(aliasSumAttr1),
      Seq(attr1))

    /* Expected data preparation */
    val expAlias1 = aliasSumAttr1.copy(aliasSumAttr1.child, aliasSumAttr1.name + "EID" +
      aliasSumAttr1.exprId.id)(aliasSumAttr1.exprId, aliasSumAttr1.qualifiers,
        aliasSumAttr1.explicitMetadata)
    val expAttr1 = AttributeReference(attr1.name, attr1.dataType)(exprId = null)

    /* Assertions */
    assertResult(expAlias1)(preparedAgg(0))
    assertResult(expAttr1)(preparedGroup(0))
  }

  test("Prepare SUM aggregation and group expressions") {
    /* Data preparation */
    val attr1 = AttributeReference("attr1", IntegerType, nullable = false)()
    val sumAttr1 = Sum(attr1)
    val aliasSumAttr1 = Alias(sumAttr1, "SumAlias")()

    /* Execution */
    val (preparedAgg, preparedGroup) = Test.prepareAggregationsUsingAliases(Seq(aliasSumAttr1),
      Seq(sumAttr1))

    /* Expected data preparation */
    val expAlias1 = aliasSumAttr1.copy(aliasSumAttr1.child, aliasSumAttr1.name + "EID" +
      aliasSumAttr1.exprId.id)(aliasSumAttr1.exprId, aliasSumAttr1.qualifiers,
        aliasSumAttr1.explicitMetadata)
    val expAttr1 = AttributeReference(expAlias1.name,
      sumAttr1.dataType)(exprId = expAlias1.exprId)

    /* Assertions */
    assertResult(expAlias1)(preparedAgg(0))
    assertResult(expAttr1)(preparedGroup(0))
  }
}

/* Helper to test the trait */
object Test extends SqlLikeRelation {

  override def tableName: String = "table"

  /* Wrapper to call the protected function */
  override def prepareAggregationsUsingAliases(aggregationExpressions:
                                               Seq[NamedExpression],
                                               groupingExpressions: Seq[Expression]):
  (Seq[NamedExpression], Seq[Expression]) = {
    super.prepareAggregationsUsingAliases(aggregationExpressions, groupingExpressions)
  }
}
