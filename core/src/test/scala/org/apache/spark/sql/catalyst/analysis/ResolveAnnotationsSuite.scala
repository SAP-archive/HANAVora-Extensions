package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar
import org.apache.spark.sql.catalyst.dsl.plans._

/**
  * Defines a set of tests of the [[ResolveAnnotations]] logic.
  */
class ResolveAnnotationsSuite extends FunSuite with MockitoSugar {

  // scalastyle:off magic.number
  val annotatedRel1 = new BaseRelation {
    override def sqlContext: SQLContext = mock[SQLContext]
    override def schema: StructType = StructType(Seq(
      StructField("id1.1", IntegerType, metadata =
        new MetadataBuilder().putLong("key1.1", 11L).build()),
      StructField("id1.2", IntegerType, metadata =
        new MetadataBuilder()
          .putLong("key1.2", 12L)
            .putLong("key1.3", 13).build()))
    )
  }
  val lr1 = LogicalRelation(annotatedRel1)
  val id11Att = lr1.output.find(_.name == "id1.1").get
  val id12Att = lr1.output.find(_.name == "id1.2").get

  val id11AnnotatedAtt = AnnotatedAttribute(id11Att)(
    Map("key1.1" -> Literal.create(100L, LongType), // override the old key
    "newkey" -> Literal.create(200L, LongType))) // define a new key

  val simpleAnnotatedSelect = lr1.select(id11AnnotatedAtt)

  test("aggregating metadata from AnnotatedAttribute to child works correctly") {
    val result = ResolveAnnotations.transferMetadataToBottom(simpleAnnotatedSelect)
    val actualAttribute = result.output.find(_.name == "id1.1").get
    assertResult(actualAttribute.metadata)(id11AnnotatedAtt.metadata)
  }

  test("collapsing annotations works correctly") {
    val result = ResolveAnnotations.collapseAnnotatedAttributes(simpleAnnotatedSelect)
    assert(result.isInstanceOf[Project])
    assert(!result.asInstanceOf[Project]
      .projectList.exists(p => p.isInstanceOf[AnnotatedAttribute]))
  }

  test("fixing attribute reference metadata works correctly") {
    val id11Alias1 = Alias(id11Att, "id11alias1")()
    val id11Alias1Anno = AnnotatedAttribute(id11Alias1)(
      Map("key1.1" -> Literal.create(100L, LongType), // override the old key
        "newkey" -> Literal.create(200L, LongType))) // define a new key
    val alias1AttrRef = AttributeReference("id11alias1ref", IntegerType, nullable = false,
        metadata = new MetadataBuilder().build())(exprId = id11Alias1.exprId)

    val id11Alias2 = Alias(id11Att, "id11alias2")()
    val id11Alias2Anno = AnnotatedAttribute(id11Alias2)(
      Map("key1.1" -> Literal.create(101L, LongType), // override the old key
        "newkey" -> Literal.create(201L, LongType))) // define a new key
    val alias2AttrRef = AttributeReference("id11alias2ref", IntegerType, nullable = false,
        metadata = new MetadataBuilder().build())(exprId = id11Alias2.exprId)

    // create a plan that has aliases (with metadata) which are referenced by attribute references.
    val plan = lr1.select(id11Alias1Anno).subquery('sq1).join(lr1.select(id11Alias2Anno)
      .subquery('sq2)).select(alias1AttrRef, alias2AttrRef)
    val result = ResolveAnnotations.fixAttributeReferencesMetadata(plan)

    // metadata will be propagated from the annotated attribute to the attribute reference
    // using the alias's id as a link.
    assertResult(result.output.find(_.name == "id11alias1ref").get.metadata) (
      id11Alias1Anno.metadata)
    assertResult(result.output.find(_.name == "id11alias2ref").get.metadata) (
      id11Alias2Anno.metadata)
  }

  test("broadcasting metadata top-down the tree works correctly") {
    val alias = Alias(id11Att, "id11alias1")()

    val finalMetadata = id11AnnotatedAtt.metadata

    // create an attribute reference of the above alias but with the final metadata which is
    // different from the alias's metadata.
    val aliasRef = AttributeReference("id11alias1ref", IntegerType, nullable = false,
      metadata = finalMetadata)(exprId = alias.exprId)

    // use the attribute reference in top level.
    val plan = lr1.select(alias).select(aliasRef)

    // now the alias's child should have the attribute reference's metadata.
    val result = ResolveAnnotations.broadcastMetadataDownTheTree(plan)

    assertResult(result.asInstanceOf[Project]
      .child.output.find(_.name == "id11alias1").get.metadata)(finalMetadata)
  }
}
