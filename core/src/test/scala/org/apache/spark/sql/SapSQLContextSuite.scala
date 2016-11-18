package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.apache.spark.sql.parser.SapParserException
import org.apache.spark.util.DummyRelationUtils._
import org.mockito.Mockito
import org.scalatest.FunSuite

class SapSQLContextSuite extends FunSuite with GlobalSapSQLContext {

  test("SQL contexts do not support hive functions") {
    val rdd = sc.parallelize(Seq(Row("1"), Row("2")))
    sqlc.createDataFrame(rdd, 'a.string, needsConversion = false)
      .registerTempTable("foo")

    intercept[AnalysisException] {
      sqlc.sql("SELECT int(a) FROM foo")
    }
  }

  test ("Check Spark Version"){
     val sap_sqlc = sqlContext.asInstanceOf[CommonSapSQLContext]
     // current spark runtime version shall be supported
     sap_sqlc.checkSparkVersion(List(org.apache.spark.SPARK_VERSION))

     // runtime exception for an unsupported version
     intercept[RuntimeException]{
      sap_sqlc.checkSparkVersion(List("some.unsupported.version"))
     }
  }

  test("Slightly different versions") {
    val sap_sqlc = sqlContext.asInstanceOf[CommonSapSQLContext]
    val spy_sap_sqlc = Mockito.spy(sap_sqlc)
    Mockito.when(spy_sap_sqlc.getCurrentSparkVersion())
      .thenReturn(org.apache.spark.SPARK_VERSION + "-CDH")

    // should not throw!
    spy_sap_sqlc.checkSparkVersion(spy_sap_sqlc.supportedVersions)

    Mockito.when(spy_sap_sqlc.getCurrentSparkVersion())
      .thenReturn("something- " + org.apache.spark.SPARK_VERSION)

    // should not throw!
    spy_sap_sqlc.checkSparkVersion(spy_sap_sqlc.supportedVersions)
  }

  test("Ensure SapSQLContext stays serializable"){
    // relevant for Bug 92818
    // Remember that all class references in SapSQLContext must be serializable!
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(sqlContext)
    oos.close()
  }

  test("Rand function") {
    sqlContext.sql(
      s"""
         |CREATE TABLE test (name varchar(20), age integer)
         |USING com.sap.spark.dstest
         |OPTIONS (
         |tableName "test"
         |)
       """.stripMargin)

    sqlContext.sql("SELECT * FROM test WHERE rand() < 0.1")
  }

  test("test version fields") {
    val sapSqlContext = sqlContext.asInstanceOf[CommonSapSQLContext]
    assert(sapSqlContext.EXTENSIONS_VERSION.isEmpty)
    assert(sapSqlContext.DATASOURCES_VERSION.isEmpty)
  }
}
