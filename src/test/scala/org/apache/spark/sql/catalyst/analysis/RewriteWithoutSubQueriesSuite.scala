package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{LogicalRelation, BaseRelation}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.scalatest.mock.MockitoSugar

class RewriteWithoutSubQueriesSuite extends FunSuite with MockitoSugar {

  val br1 = new BaseRelation {
    override def sqlContext: SQLContext = mock[SQLContext]
    override def schema: StructType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
  }

  val lr1 = LogicalRelation(br1)
  val nameAtt = lr1.output.find(_.name == "name").get
  val ageAtt = lr1.output.find(_.name == "age").get

  val lr2 = LogicalRelation(br1)
  val nameAtt2 = lr2.output.find(_.name == "name").get
  val ageAtt2 = lr2.output.find(_.name == "age").get

  test("rewrite subqueries") {
    assertResult(lr1)(RewriteWithoutSubQueries(lr1.subquery('q)))
    assertResult(lr1.select(nameAtt))(RewriteWithoutSubQueries(
      lr1.subquery('q).select(nameAtt.withQualifiers("q" :: Nil))
    ))
    assertResult(
      lr1.select(nameAtt.withQualifiers("foo" :: Nil), ageAtt)
        .select(nameAtt.withQualifiers("foo" :: Nil))
    )(RewriteWithoutSubQueries(
      lr1.select(nameAtt.withQualifiers("foo" :: Nil), ageAtt)
        .subquery('q).select(nameAtt.withQualifiers("q" :: Nil))
    ))
    assertResult(
      lr1.join(lr2).select(nameAtt, nameAtt2)
    )(RewriteWithoutSubQueries(
      lr1.subquery('q1).join(lr2.subquery('q2))
        .select(nameAtt.withQualifiers("q1" :: Nil), nameAtt2.withQualifiers("q2" :: Nil))
    ))
    assertResult(
      lr1.join(lr2).select(nameAtt2, nameAtt)
    )(RewriteWithoutSubQueries(
      lr1.subquery('q1).join(lr2.subquery('q2))
        .select(nameAtt2.withQualifiers("q2" :: Nil), nameAtt.withQualifiers("q1" :: Nil))
    ))
  }

}
