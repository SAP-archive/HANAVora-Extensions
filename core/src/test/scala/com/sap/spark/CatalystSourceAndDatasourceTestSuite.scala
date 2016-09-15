package com.sap.spark

import org.apache.spark.sql.execution.tablefunctions.TPCHTables
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.apache.spark.util.DummyRelationUtils._
import org.scalatest.FunSuite
import com.sap.spark.util.TestUtils._

/**
  * This uses the com.sap.spark.catalystSourceTest Default source to test the pushdown of plans
  */
// scalastyle:off magic.number
class CatalystSourceAndDatasourceTestSuite
  extends FunSuite
  with GlobalSapSQLContext {

  test("Select with group by (bug 116823)") {
    val rdd = sc.parallelize(Seq(Row("a", 1L)))
    registerMockCatalystRelation(
      tableName = "foo",
      schema = StructType('a1.double :: 'a2.int :: 'a3.string :: Nil),
      data = rdd)

    val df = sqlc.sql("SELECT a3 AS MYALIAS, COUNT(a1) FROM foo GROUP BY a3")

    assertResult(Array(Row("a", 1)))(df.collect())
  }

  test("Select with group by and having (bug 116824)") {
    val rdd = sc.parallelize(Seq(Row(900L, "a"), Row(101L, "a"), Row(1L, "b")), numSlices = 3)
    val ordersSchema = TPCHTables(sqlc).ordersSchema
    registerMockCatalystRelation("ORDERS", ordersSchema, rdd)

    val df = sqlc.sql(
      """SELECT O_ORDERSTATUS, count(O_ORDERKEY) AS NumberOfOrders
      |FROM ORDERS
      |GROUP BY O_ORDERSTATUS
      |HAVING count(O_ORDERKEY) > 1000""".stripMargin)

    assertResult(Seq(Row("a", 1001L)))(df.collect().toSeq)
  }

  test("Average pushdown"){
    val rdd = sc.parallelize(
      Seq(Row("name1", 20.0, 10L), Row("name2", 10.0, 10L),
          Row("name1", 10.0, 10L), Row("name2", 20.0, 10L)),
      numSlices = 2)
    registerMockCatalystRelation("persons", StructType('name.string :: 'age.int :: Nil), rdd)

    val result =
      sqlContext.sql(s"SELECT name, avg(age) FROM persons GROUP BY name").collect().toSet

    assertResult(Set(Row("name1", 1.5), Row("name2", 1.5)))(result)
 }

  test("Nested query") {
    val rdd = sc.parallelize(Seq(Row(5), Row(5), Row(5), Row(1)), numSlices = 2)
    registerMockCatalystRelation(
      tableName = "fourColumns",
      StructType(('a' to 'e').map(char => Symbol(char.toString).int)),
      data = rdd)

    val result = sqlContext.sql(s"SELECT COUNT(*) FROM (SELECT e,sum(d) " +
      s"FROM fourColumns GROUP BY e) as A").collect().toSet

    assertResult(Set(Row(2)))(result)
  }
}
