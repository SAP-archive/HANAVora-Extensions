package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.sap.spark.dstest.DummyRelationWithoutTempFlag
import org.scalatest.FunSuite

class SapSQLContextSuite extends FunSuite {

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
    var sapSQLContext = SapSQLContextEnv.getContext()

    valid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sapSQLContext.sql(stmt)
      }
    }
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sapSQLContext.sql(stmt)
      }
    }

    // should fail if "spark.vora.ignore_use_statements" prop is "false"
    sapSQLContext = SapSQLContextEnv.getContext(Map(
      SapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS -> "false"
    ))
    valid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sapSQLContext.sql(stmt)
      }
    }
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sapSQLContext.sql(stmt)
      }
    }

    // should fail if "spark.vora.ignore_use_statements" prop is "true"
    sapSQLContext = SapSQLContextEnv.getContext(Map(
      SapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS -> "true"
    ))
    valid_use_statements.foreach { stmt =>
      sapSQLContext.sql(stmt)
    }
    // these should still fail even if "spark.vora.ignore_use_statements" prop is "true"
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sapSQLContext.sql(stmt)
      }
    }
  }

  test("Auto-registering Feature ON") {
    val relationName = "TestRelation"
    com.sap.spark.dstest.DefaultSource.addRelation(relationName)
    val sapSqlContext =
      SapSQLContextEnv.getContext(Map
        (SapSQLContext.PROPERTY_AUTO_REGISTER_TABLES -> "com.sap.spark.dstest"))

    val tables = sapSqlContext.tableNames()
    assert(tables.contains(relationName))
  }

  test("Auto-registering Feature OFF") {
    val sapSqlContext =
      SapSQLContextEnv.getContext()
    val tables = sapSqlContext.tableNames()
    assert(tables.length == 0)
  }

  test("Ensure SapSQLContext stays serializable"){
    // relevant for Bug 92818
    // Remember that all class references in SapSQLContext must be serializable!
    val sapSqlContext = SapSQLContextEnv.getContext()
    val oos = new ObjectOutputStream(new ByteArrayOutputStream())
    oos.writeObject(sapSqlContext)
    oos.close
  }


}
