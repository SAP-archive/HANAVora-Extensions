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
}
