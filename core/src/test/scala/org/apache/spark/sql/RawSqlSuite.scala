package org.apache.spark.sql

import org.apache.spark.sql.catalyst.analysis.ResolveSelectUsing
import org.apache.spark.sql.catalyst.plans.logical.{SelectUsing, Statistics, UnresolvedSelectUsing}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.RawSqlSourceStrategy
import org.apache.spark.sql.sources.{LazySchemaRawSqlExecution, RawSqlExecution, RawSqlSourceProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.DummyRelationUtils._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

import scala.reflect.ClassTag

class RawSqlSuite extends FunSuite with GlobalSapSQLContext with MockitoSugar {

  def withRawSqlProvider[A](f: RawSqlSourceProvider => A): A = {
    val resolver = mock[DatasourceResolver]
    val provider = mock[RawSqlSourceProvider]
    DatasourceResolver.withResolver(sqlc, resolver) {
      when(resolver.newInstanceOfTyped(any[String])(any[ClassTag[RawSqlSourceProvider]]))
        .thenReturn(provider)
      f(provider)
    }
  }

  val rawSqlString = "THIS IS THE RAW SQL STRING"
  val className = "com.sap.spark.dsmock"

  test("Resolve a correct UnresolvedSelectUsing") {
    withRawSqlProvider { provider =>
      val mockExecution = mock[RawSqlExecution]
      when(
        provider.executionOf(
          any[SQLContext],
          any[Map[String, String]],
          any[String],
          any[Option[StructType]]))
        .thenReturn(mockExecution)
      val unresolvedPlan = UnresolvedSelectUsing(rawSqlString, className)

      val resolvedPlan = ResolveSelectUsing(sqlc).apply(unresolvedPlan)

      assertResult(SelectUsing(mockExecution))(resolvedPlan)
    }
  }

  test("Resolved select with has 'statistics'") {
    withRawSqlProvider { provider =>
      val execution = mock[RawSqlExecution]
      val statistics = mock[Statistics]
      when(execution.statistics).thenReturn(statistics)
      when(provider.executionOf(
        any[SQLContext],
        any[Map[String, String]],
        any[String],
        any[Option[StructType]]))
        .thenReturn(execution)
      val unresolvedPlan =
        UnresolvedSelectUsing(
          rawSqlString,
          className,
          Some(StructType(Seq.empty)))

      val resolvedPlan = ResolveSelectUsing(sqlc).apply(unresolvedPlan)

      // the 'statistics' call would throw if not implemented!
      assertResult(statistics)(resolvedPlan.statistics)
    }
  }

  test("(Physical) Strategy transforms correctly") {
    withRawSqlProvider { provider =>
      val sparkPlan = mock[SparkPlan]
      val execution = mock[RawSqlExecution]
      when(execution.createSparkPlan())
        .thenReturn(sparkPlan)
      val logicalPlan = SelectUsing(execution)

      val Seq(plan) = RawSqlSourceStrategy(logicalPlan)

      assertResult(sparkPlan)(plan)
    }
  }

  /**
    * This test is supposed to test the 'full stack' from parsing, over physical planning
    */
  test(s"RawSQL test with Parsing, Logcial Plan, and Physical Planning without specified " +
    s"schema. Left Delimiter ``, Right Delimiter ``"){

    withRawSqlProvider { provider =>
      val expectedSchema = StructType('a.int :: 'b.int :: Nil)
      val expectedValues = Set(Row(0, 0), Row(0, 1))
      val execution = new RawSqlExecution {
        override def sqlContext: SQLContext = RawSqlSuite.this.sqlContext

        override def schema: StructType = expectedSchema

        override def createSparkPlan(): SparkPlan =
          rowRddToSparkPlan(sc.parallelize(expectedValues.toSeq))
      }

      when(provider.executionOf(sqlContext, Map.empty, rawSqlString, None))
        .thenReturn(execution)

      val df = sqlc.sql(s"""``$rawSqlString`` USING $className""")

      assertResult(expectedSchema)(df.schema)
      assertResult(expectedValues)(df.collect().toSet)
    }
  }

  test("LazySchemaRawSqlExecution computes schema lazily") {
    def onlyForTesting: Nothing =
      throw new NotImplementedError(
        "This method was created only for testing purposes and should not be accessed")

    val ex = new LazySchemaRawSqlExecution {
      override def calculateSchema(): StructType = 'a.string

      override def sqlContext: SQLContext = onlyForTesting

      override def createSparkPlan(): SparkPlan = onlyForTesting
    }

    assertResult(None)(ex.schemaOption)
    assertResult('a.string: StructType)(ex.schema)
    assertResult(Some('a.string: StructType))(ex.schemaOption)
  }
}
