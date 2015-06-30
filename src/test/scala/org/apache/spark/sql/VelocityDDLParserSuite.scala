package org.apache.spark.sql

import corp.sap.spark.datasourcecatalogtest.DefaultSource
import corp.sap.spark.{GlobalSparkContext, WithSparkContext}
import org.apache.spark.{MockitoSparkContext, Logging}
import org.scalatest.{ConfigMap, FunSuite, BeforeAndAfterAll}
import org.apache.spark.sql.types.{StructType,StructField,StringType};

class VelocityDDLParserSuite extends FunSuite with GlobalSparkContext {

  @Override
  override def beforeAll() : Unit = {
    super.beforeAll()
    DefaultSource.addTable("testtable")
    DefaultSource.addTable("testtable2")
  }

  @Override
  override def afterAll() : Unit = {
    super.afterAll()
    DefaultSource.reset()
  }

  test("Show vtables parser test without options"){
    val sqlContext = new VelocitySQLContext(sc)


    val result = sqlContext.sql(
      s"""SHOW DATASOURCETABLES
         |USING corp.sap.spark.datasourcecatalogtest""".stripMargin).collect()

    assert(result.contains(Row("testtable")))
    assert(result.contains(Row("testtable2")))
    assert(result.size == 2)
  }

  test("Show vtables parser test with options"){
    val sqlContext = new VelocitySQLContext(sc)

    val result = sqlContext.sql(
      s"""SHOW DATASOURCETABLES
         |USING corp.sap.spark.datasourcecatalogtest
         |OPTIONS(key "value")""".stripMargin).collect()

    assert(result.contains(Row("testtable")))
    assert(result.contains(Row("testtable2")))
    assert(result.size == 2)
    assert(DefaultSource.passedOptions.get("key") == Some("value"))
  }
}
