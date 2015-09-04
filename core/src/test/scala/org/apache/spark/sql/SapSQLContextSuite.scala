package org.apache.spark.sql

import org.scalatest.FunSuite


class SapSQLContextSuite extends FunSuite
  with GlobalVelocitySQLContext {

  test("Ignore USE keyword") {
    // Every valid "USE xyz" statement should produce a
    // UseStatementCommand. However, if the
    // "ignore_use_statement" flag is missing
    // or false, a SapParseError is thrown
    // by the command on execution time.

    val valid_use_statements = List(
      "USE DATABASE dude",
      "USE DATABASE a b c sdf sdfklasdfjklsd",
      "USE xyt dude",
      "USE      "
    )

    val invalid_use_statements = List(
      "asdsd USE    ",
      "CREATE TABLE USE tab001" // USE is now a keyword
    )

    // should fail if "ignore_use_statements" flag is missing
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

    // should fail if "ignore_use_statements" flag is false
    sqlContext.setConf(SapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "false")
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

    // should pass if "ignore_use_statements" flag is true
    sqlContext.setConf(SapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "true")
    valid_use_statements.foreach { stmt =>
      sqlContext.sql(stmt)
    }
    // these should still fail even if "ignore_use_statements" flag is true
    invalid_use_statements.foreach { stmt =>
      intercept[SapParserException] {
        sqlContext.sql(stmt)
      }
    }
  }
}
