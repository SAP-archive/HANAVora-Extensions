package org.apache.spark.sql.catalyst.analysis

import com.sap.spark.dsmock.DefaultSource
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{SelectWith, UnresolvedSelectWith}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
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

  test("Resolve with an existing schema") {
    testWithMockedSource {
      val fields = Seq(StructField("a", IntegerType))
      val unresolvedPlan = UnresolvedSelectWith(rawSqlString, className,
        Some(fields))
      val analyzer = mock(classOf[Analyzer])

      val resolvedPlan = ResolveSelectWith(analyzer).apply(unresolvedPlan)

      assert(resolvedPlan.isInstanceOf[SelectWith])
      val typedPlan = resolvedPlan.asInstanceOf[SelectWith]
      assert(typedPlan.sqlCommand == rawSqlString && typedPlan.className == className)
      // scalastyle:off magic.number
      // Note: we cannot check against real attributes because they will create a different expr. id
      assert(typedPlan.output(0).name == fields(0).name
        && typedPlan.output(0).dataType == fields(0).dataType)
      // scalastyle:on
    }
  }

  test("Resolve with an empty schema") {
    testWithMockedSource {
      val unresolvedPlan = UnresolvedSelectWith(rawSqlString, className,
        Some(Seq.empty))
      val analyzer = mock(classOf[Analyzer])

      val resolvedPlan = ResolveSelectWith(analyzer).apply(unresolvedPlan)

      assert(resolvedPlan == SelectWith(rawSqlString, className, Seq.empty))
    }
  }
}
