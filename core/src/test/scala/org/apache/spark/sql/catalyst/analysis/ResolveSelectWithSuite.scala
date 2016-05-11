package org.apache.spark.sql.catalyst.analysis

import com.sap.spark.dsmock.DefaultSource
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{SelectWith, UnresolvedSelectWith}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite

class ResolveSelectWithSuite extends FunSuite {

  def testWithMockedSource(block: => Unit): Unit = {
    DefaultSource.withMock{ defaultSource =>
      when(defaultSource.getResultingAttributes(
        anyObject[String]))
        .thenReturn(attributes)
      block
    }
  }

  val rawSqlString = "THIS IS THE RAW SQL STRING"
  val className = "com.sap.spark.dsmock"
  val attributes = Seq(AttributeReference("a", IntegerType)(),
    AttributeReference("b", StringType)())

  test("Resolve a correct UnresolvedSelectWith") {
    testWithMockedSource {
      val unresolvedPlan = UnresolvedSelectWith(rawSqlString, className)
      val analyzer = mock(classOf[Analyzer])

      val resolvedPlan = ResolveSelectWith(analyzer).apply(unresolvedPlan)

      assert(resolvedPlan == SelectWith(rawSqlString, className, attributes))
    }
  }

  test("Resolve with a non existant default source will throw") {
    testWithMockedSource {
      val unresolvedPlan = UnresolvedSelectWith(rawSqlString, "non.existant.class")
      val analyzer = mock(classOf[Analyzer])

      intercept[AnalysisException](ResolveSelectWith(analyzer).apply(unresolvedPlan))
    }
  }

  test("Resolve with an existant default source but not implementing the RawSqlSource Provider " +
    "Interface will throw") {
    testWithMockedSource {
      val unresolvedPlan = UnresolvedSelectWith(rawSqlString, "com.sap.spark.dstest")
      val analyzer = mock(classOf[Analyzer])

      intercept[AnalysisException](ResolveSelectWith(analyzer).apply(unresolvedPlan))
    }
  }
}
