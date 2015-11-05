package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import org.scalatest.FunSuite

class SapSQLContextSuite extends FunSuite with GlobalSapSQLContext {

  test("Ignore USE keyword") {
    // Behaviour:
    // Every syntactically correct "USE [xyz...]" statement produces a UseStatementCommand.
    // If the spark "ignore_use_statement" property is missing or false
    // a SapParseError is thrown, else, the statement is ignored.

    val valid_use_statements = List(
      "USE DATABASE dude",
      "USE DATABASE a b c sdf sdfklasdfjklsd",
      "USE xyt dude",
      "USE      "
    )

    val invalid_use_statements = List(
      "asdsd USE    ",
      "CREATE TABLE USE tab001" // fails since USE is now a keyword
    )

    // should fail if "spark.vora.ignore_use_statements" prop is missing
    valid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }

    // should fail if "spark.vora.ignore_use_statements" prop is "false"
    sqlContext.setConf(
      AbstractSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "false")
    valid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }

    // should fail if "spark.vora.ignore_use_statements" prop is "true"
    sqlContext.setConf(
      AbstractSapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "true")
    valid_use_statements.foreach { stmt =>
      sqlContext.sql(stmt)
    }
    // these should still fail even if "spark.vora.ignore_use_statements" prop is "true"
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }
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
         |tableName "test",
         |local "true",
         |hosts "localhost"
         |)
       """.stripMargin)

    sqlContext.sql("SELECT * FROM test WHERE rand() < 0.1")
  }
}
