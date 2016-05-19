package com.sap.spark.cli

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream}

import org.apache.spark.SparkContext
import org.apache.spark.sql.{GlobalSapSQLContext, SQLContext}
import org.scalatest.{BeforeAndAfterEach, FunSuite, ShouldMatchers}

/**
  * Test the SQLRunner class.
  */
class SQLRunnerSuite extends FunSuite
  with BeforeAndAfterEach
  with ShouldMatchers
  with GlobalSapSQLContext {

  override def beforeEach(): Unit = {
    super.beforeEach()

    val records = List((1, "a"), (2, "b"), (3, "c"))
    val df = sqlContext.createDataFrame(records)

    df.registerTempTable("DEMO_TABLE")
  }

  override def afterEach(): Unit = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    sqlContext.sql("DROP TABLE IF EXISTS DEMO_TABLE")

    super.afterEach()
  }

  test("can parse command line arguments") {
    /*
     * The sqlrunner accepts 1 or more sql files, and 0 or 1 (--output, file) pairs.
     */

    // good call
    val goodOpts =
      SQLRunner.parseOpts(List("a.sql", "b.sql", "-o", "output.csv"))

    goodOpts.sqlFiles should be(List("a.sql", "b.sql"))
    goodOpts.output should be(Some("output.csv"))

    // bad call
    val badOpts = SQLRunner.parseOpts(List())

    badOpts.sqlFiles should be(List())
    badOpts.output should be(None)

    // ugly call
    val uglyOpts =
      SQLRunner.parseOpts(List("a.sql", "-o", "output.csv", "b.sql"))

    uglyOpts.sqlFiles should be(List("a.sql", "b.sql"))
    uglyOpts.output should be(Some("output.csv"))
  }

  def runSQLTest(input: String, expectedOutput: String): Unit = {
    val inputStream: InputStream = new ByteArrayInputStream(input.getBytes())
    val outputStream = new ByteArrayOutputStream()

    SQLRunner.sql(inputStream, outputStream)

    val output = outputStream.toString
    output should be(expectedOutput)
  }

  test("can run dummy query") {
    val input = "SELECT 1;"
    val output = "1\n"

    runSQLTest(input, output)
  }

  test("can run multiple dummy queries") {
    val input = """
        |SELECT 1;SELECT 2;
        |SELECT 3;
      """.stripMargin

    val output = "1\n2\n3\n"

    runSQLTest(input, output)
  }

  test("can run a basic example with tables") {
    val input = """
                  |SELECT * FROM DEMO_TABLE;
                  |SELECT * FROM DEMO_TABLE LIMIT 1;
                  |DROP TABLE DEMO_TABLE;
                """.stripMargin

    val output = "1,a\n2,b\n3,c\n1,a\n"

    runSQLTest(input, output)
  }

  test("can run an example with comments") {
    val input = """
                  |SELECT * FROM DEMO_TABLE; -- this is the first query
                  |SELECT * FROM DEMO_TABLE LIMIT 1;
                  |-- now let's drop a table
                  |DROP TABLE DEMO_TABLE;
                """.stripMargin

    val output = "1,a\n2,b\n3,c\n1,a\n"

    runSQLTest(input, output)
  }
}
