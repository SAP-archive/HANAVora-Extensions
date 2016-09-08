package org.apache.spark.sql

import org.apache.spark.sql.DatasourceResolver.withResolver
import org.apache.spark.sql.parser.SapParserException
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.util.DummyRelationUtils._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.internal.stubbing.answers.Returns
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class CommandSuite
  extends FunSuite
  with MockitoSugar
  with GlobalSapSQLContext {

  test("append command") {
    abstract class MockAppendRelation extends BaseRelation with AppendRelation
    val relation = mock[MockAppendRelation]
    when(relation.schema).thenReturn('a.string)
    sqlc.baseRelationToDataFrame(relation).registerTempTable("foo")

    sqlc.sql("APPEND TABLE foo OPTIONS (foo \"bar\")")

    verify(relation, times(1)).appendFilesToTable(Map("foo" -> "bar"))
  }

  test("SHOW TABLES USING retrieves correct information") {
    val datasource = mock[DatasourceCatalog]
    when(datasource.getRelations(any[SQLContext], any[Map[String, String]]))
      .thenReturn(
        RelationInfo("t1", isTemporary = false, "TABLE", None, "my.custom.resolver") ::
          RelationInfo("t2", isTemporary = true, "TABLE", None, "") ::
          RelationInfo("v1", isTemporary = false, "VIEW", Some("ddl"), "my.custom.resolver") ::
          RelationInfo("v2", isTemporary = true, "VIEW", Some("ddl"), "") :: Nil)
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("my.custom.resolver"))
      .thenAnswer(new Returns(datasource))

    withResolver(sqlc, resolver) {
      val values =
        sqlc.sql("SHOW TABLES USING my.custom.resolver").collect().toSet

      assertResult(Set(
        Row("t1", "FALSE", "TABLE"),
        Row("t2", "TRUE", "TABLE"),
        Row("v1", "FALSE", "VIEW"),
        Row("v2", "TRUE", "VIEW")))(values)
    }
  }

  test("CREATE HASH PARTITION FUNCTION command") {
    val provider = mock[PartitioningFunctionProvider]
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitioningFunctionProvider]("bar"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      sqlc.sql(
        """CREATE PARTITION FUNCTION foo (integer)
          |AS HASH
          |USING bar""".stripMargin)
    }

    verify(provider, times(1))
      .createHashPartitioningFunction(
        sqlc,
        Map.empty,
        "foo",
        Seq(IntegerType),
        None)
  }

  test("CREATE RANGE SPLIT PARTITION FUNCTION command") {
    val provider = mock[PartitioningFunctionProvider]
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitioningFunctionProvider]("bar"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      sqlc.sql(
        """CREATE PARTITION FUNCTION foo (integer)
          |AS RANGE
          |SPLITTERS (5, 10, 15)
          |USING bar""".stripMargin)
    }

    verify(provider, times(1))
      .createRangeSplitPartitioningFunction(
        sqlc,
        Map.empty,
        "foo",
        IntegerType,
        Seq(5, 10, 15), // scalastyle:ignore magic.number
        rightClosed = false)
  }

  test("CREATE RANGE INTERVAL PARTITION FUNCTION command") {
    val provider = mock[PartitioningFunctionProvider]
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitioningFunctionProvider]("bar"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      sqlc.sql(
        """CREATE PARTITION FUNCTION foo (integer)
          |AS RANGE
          |START 5
          |END 25
          |PARTS 3
          |USING bar""".stripMargin)
    }

    verify(provider, times(1))
      .createRangeIntervalPartitioningFunction(
        sqlc,
        Map.empty,
        "foo",
        IntegerType,
        start = 5, // scalastyle:ignore magic.number
        end = 25, // scalastyle:ignore magic.number
        Right(3))
  }

  test("DROP PARTITION FUNCTION command") {
    val provider = mock[PartitioningFunctionProvider]
    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOfTyped[PartitioningFunctionProvider]("bar"))
      .thenReturn(provider)

    withResolver(sqlc, resolver) {
      sqlc.sql(
        """DROP PARTITION FUNCTION foo
          |USING bar""".stripMargin)
    }

    verify(provider, times(1))
      .dropPartitioningFunction(sqlc, Map.empty, "foo", allowNotExisting = false)
  }

  test("USE statements should not do anything when ignored") {
    // This property is only used in this command and unfortunately has no default value in spark
    // which is why we set it here and don't do any cleanup.
    sqlContext.setConf(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "true")
    sqlc.sql("USE foo bar")
  }

  test("Any other use command should throw an exception") {
    sqlContext.setConf(CommonSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "false")
    intercept[SapParserException] {
      sqlc.sql("USE foo bar")
    }
  }
}
