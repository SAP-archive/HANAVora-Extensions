package com.sap.spark

import com.sap.spark.catalystSourceTest.{CatalystSourceTestRDD, CataystSourceTestRDDPartition}
import org.apache.spark.sql.DatasourceResolver.withResolver
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.sql.SqlLikeRelation
import org.apache.spark.sql.sources.{BaseRelation, CatalystSource, TemporaryAndPersistentRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DatasourceResolver, GlobalSapSQLContext, Row, SQLContext}
import org.apache.spark.util.DummyRelationUtils._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.internal.stubbing.answers.Returns
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

/**
  * This uses the com.sap.spark.catalystSourceTest Default source to test the pushdown of plans
  */
// scalastyle:off magic.number file.length
class CatalystSourceAndDatasourceTestSuite
  extends FunSuite
  with GlobalSapSQLContext
  with MockitoSugar{

  val testTableNameNameAge = "testTableNameAge"
  val testTableFourIntColumns = "testTableFourIntCols"

  private def createTestTableNameAge(sqlc: SQLContext) = {
    sqlc.sql(
      s"""CREATE TABLE $testTableNameNameAge (name string, age integer)
          |USING com.sap.spark.catalystSourceTest
          |OPTIONS ()""".stripMargin)
  }

  private def createTestTableFourIntColumns(sqlc: SQLContext) = {
    sqlc.sql(
      s"""CREATE TABLE $testTableFourIntColumns (a integer, b integer, c integer,
          |d integer, e integer)
          |USING com.sap.spark.catalystSourceTest
          |OPTIONS ()""".stripMargin)
  }

  test("Select with group by (bug 116823)") {
    abstract class MockRelation extends BaseRelation with SqlLikeRelation with CatalystSource
    val rdd = sc.parallelize(Seq(Row("a", 1L)))
    val relation = mock[MockRelation]
    val resolver = mock[DatasourceResolver]
    val provider = mock[TemporaryAndPersistentRelationProvider]
    when(relation.supportsLogicalPlan(any[LogicalPlan])).thenReturn(true)
    when(relation.logicalPlanToRDD(any[LogicalPlan])).thenReturn(rdd)
    when(relation.schema)
      .thenReturn(StructType('a1.double :: 'a2.int :: 'a3.string :: Nil))
    when(relation.isMultiplePartitionExecution(any[Seq[CatalystSource]]))
      .thenReturn(true)
    when(provider.createRelation(
      sqlContext = any[SQLContext],
      tableName = any[Seq[String]],
      parameters = any[Map[String, String]],
      isTemporary = any[Boolean],
      allowExisting = any[Boolean]))
      .thenReturn(relation)
    when(resolver.newInstanceOf("com.sap.spark.vora"))
      .thenAnswer(new Returns(provider))

    withResolver(sqlc, resolver) {
      sqlc.sql("CREATE TABLE foo (a1 double, a2 int, a3 string) USING com.sap.spark.vora")
      val df = sqlc.sql("SELECT a3 AS MYALIAS, COUNT(a1) FROM foo GROUP BY a3")
      assertResult(Array(Row("a", 1)))(df.collect())
    }
  }

  test("Average pushdown"){

    createTestTableNameAge(sqlContext)

    CatalystSourceTestRDD.partitions =
      Seq(CataystSourceTestRDDPartition(0),CataystSourceTestRDDPartition(1))
    CatalystSourceTestRDD.rowData =
      Map(CataystSourceTestRDDPartition(0) -> Seq(Row("name1", 20.0, 10L), Row("name2", 10.0, 10L)),
        CataystSourceTestRDDPartition(1) -> Seq(Row("name1", 10.0, 10L), Row("name2", 20.0, 10L)))

    val result =
      sqlContext.sql(s"SELECT name, avg(age) FROM $testTableNameNameAge GROUP BY name").collect()

    assert(result.size == 2)
    assert(result.contains(Row("name1", 1.5)))
    assert(result.contains(Row("name2", 1.5)))
 }

  test("Nested query") {
    createTestTableFourIntColumns(sqlContext)

    CatalystSourceTestRDD.partitions =
      Seq(CataystSourceTestRDDPartition(0),CataystSourceTestRDDPartition(1))
    CatalystSourceTestRDD.rowData =
      Map(CataystSourceTestRDDPartition(0) -> Seq(Row(5), Row(5)),
        CataystSourceTestRDDPartition(1) -> Seq(Row(5), Row(1)))

    val result = sqlContext.sql(s"SELECT COUNT(*) FROM (SELECT e,sum(d) " +
      s"FROM ${testTableFourIntColumns} GROUP BY e) as A").collect()

    assert(result.size == 1)
    assert(result.contains(Row(2)))
  }
}
