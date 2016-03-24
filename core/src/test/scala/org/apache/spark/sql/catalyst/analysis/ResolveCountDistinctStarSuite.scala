package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Alias, CountDistinct}
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Inside._
import org.scalatest.mock.MockitoSugar
import org.apache.spark.sql.catalyst.dsl.plans.DslLogicalPlan

import scala.collection.mutable.ArrayBuffer

class ResolveCountDistinctStarSuite extends FunSuite with MockitoSugar {
  val persons = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = mock[SQLContext]
    override def schema: StructType = StructType(Seq(
      StructField("age", IntegerType),
      StructField("name", StringType)
    ))
  })

  test("Count distinct star is resolved correctly") {
    val projection = persons.select(UnresolvedAlias(CountDistinct(UnresolvedStar(None) :: Nil)))
    val stillNotCompletelyResolvedAggregate = SimpleAnalyzer.execute(projection)
    val resolvedAggregate = ResolveCountDistinctStar(SimpleAnalyzer)
                              .apply(stillNotCompletelyResolvedAggregate)
    inside(resolvedAggregate) {
      case Aggregate(Nil, ArrayBuffer(Alias(CountDistinct(expressions), _)), _) =>
        assert(expressions.collect {
          case a:AttributeReference => a.name
        }.toSet == Set("name", "age"))
    }
    assert(resolvedAggregate.resolved)
  }
}
