package org.apache.spark.sql.sources

import org.apache.spark.Logging
import org.apache.spark.sql.{VelocityParserException, VelocitySqlParser}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSuite, GivenWhenThen}

class VelocityDDLParserSuite
  extends FunSuite
  with TableDrivenPropertyChecks
  with GivenWhenThen
  with Logging {

  val sqlParser = new VelocitySqlParser
  val ddlParser = new VelocityDDLParser(sqlParser.parse)

  val showDatasourceTablesPermutations = Table(
    ("sql", "provider", "options", "willFail"),
    ("SHOW DATASOURCETABLES USING com.provider", "com.provider", Map.empty[String, String], false),
    ("SHOW DATASOURCETABLES USING com.provider OPTIONS(key \"value\")",
      "com.provider", Map("key" -> "value"), false),
    ("SHOW DATASOURCETABLES", "", Map.empty[String, String], true)
  )

  test("SHOW DATASOURCETABLES command") {
    forAll(showDatasourceTablesPermutations) { (sql, provider, options, willFail) =>

      Given(s"provider: $provider, options: $options, sql: $sql, willFail: $willFail")

      if (willFail) {
        intercept[VelocityParserException] {
          ddlParser.parse(sql)
        }
      } else {
        val result = ddlParser.parse(sql)

        Then("it will be an instance of ShowDatasourceTablesCommand class")
        assert(result.isInstanceOf[ShowDatasourceTablesCommand])

        val instancedResult = result.asInstanceOf[ShowDatasourceTablesCommand]

        Then("options will be equals")
        assert(instancedResult.options == options)
        Then("provider will be equals")
        assert(instancedResult.classIdentifier == provider)
      }
    }
  }

  val registerAllTablesCommandPermutations =
    Table(
      ("sql", "provider", "options", "ignoreConflicts"),
      ("REGISTER ALL TABLES USING provider.name OPTIONS() IGNORING CONFLICTS",
        "provider.name", Map.empty[String, String], true),
      ( """REGISTER ALL TABLES USING provider.name OPTIONS(optionA "option")""",
        "provider.name", Map("optionA" -> "option"), false),
      ( """REGISTER ALL TABLES USING provider.name""",
        "provider.name", Map.empty[String, String], false),
      ( """REGISTER ALL TABLES USING provider.name IGNORING CONFLICTS""",
        "provider.name", Map.empty[String, String], true)
    )

  test("REGISTER ALL TABLES command") {
    forAll(registerAllTablesCommandPermutations) {
      (sql: String, provider: String, options: Map[String, String], ignoreConflicts: Boolean) =>
        Given(s"provider: $provider, options: $options, ignoreConflicts: $ignoreConflicts")
        val result = ddlParser.parse(sql)

        Then("the result will be a instance of RegisterAllTablesUsing")
        assert(result.isInstanceOf[RegisterAllTablesUsing])

        val convertedResult = result.asInstanceOf[RegisterAllTablesUsing]

        Then("the ignoreConflicts will be correct")
        assert(convertedResult.ignoreConflicts == ignoreConflicts)
        Then("the options will be correct")
        assert(convertedResult.options == options)
        Then("the provider name will be correct")
        assert(convertedResult.provider == provider)
    }
  }

  val registerTableCommandPermutations =
    Table(
      ("sql", "table", "provider", "options", "ignoreConflicts"),
      ("REGISTER TABLE bla USING provider.name OPTIONS() IGNORING CONFLICTS",
        "bla", "provider.name", Map.empty[String, String], true),
      ("""REGISTER TABLE bla USING provider.name OPTIONS(optionA "option")""",
        "bla", "provider.name", Map("optionA" -> "option"), false),
      ("""REGISTER TABLE bla USING provider.name""",
        "bla", "provider.name", Map.empty[String, String], false),
      ("""REGISTER TABLE bla USING provider.name IGNORING CONFLICTS""",
        "bla", "provider.name", Map.empty[String, String], true)
    )

  test("REGISTER TABLE command") {
    forAll(registerTableCommandPermutations) {
      (sql: String, table: String, provider: String, options: Map[String, String],
       ignoreConflict: Boolean) =>
        Given(s"provider: $provider, options: $options, ignoreConflicts: $ignoreConflict")
        val result = ddlParser.parse(sql)

        Then("the result will be a instance of RegisterAllTablesUsing")
        assert(result.isInstanceOf[RegisterTableUsing])

        val convertedResult = result.asInstanceOf[RegisterTableUsing]

        Then("the table name is correct")
        assert(convertedResult.tableName == table)
        Then("the ignoreConflicts will be correct")
        assert(convertedResult.ignoreConflict == ignoreConflict)
        Then("the options will be correct")
        assert(convertedResult.options == options)
        Then("the provider name will be correct")
        assert(convertedResult.provider == provider)
    }
  }

  test("test DDL of Bug 90774") {
    val testTable = """
CREATE TEMPORARY TABLE testBaldat (field1 string, field2 string, field3 string,
  field4 string, field5 integer, field6 string, field7 integer)
USING com.sap.spark.velocity
OPTIONS (
  tableName "testBaldat",
  paths "/user/u1234/data.csv",
  hosts "a1.b.c.d.com,a2.b.c.d.com,a3.b.c.d.com",
  zkurls "a1.b.c.d.com:2181,a2.b.c.d.com:2181",
  nameNodeUrl "a5.b.c.d.com:8020"
)"""
    ddlParser.parse(testTable, ddlExceptionOnError = true)
    ddlParser.parse(testTable, ddlExceptionOnError = false)
  }

  test("test simple CREATE TEMPORARY TABLE (Bug 90774)") {
    val testTable = """CREATE TEMPORARY TABLE tab001(a int)
      USING a.b.c.d
      OPTIONS (
        tableName "blaaa"
      )"""
    ddlParser.parse(testTable, ddlExceptionOnError = true)
    ddlParser.parse(testTable, ddlExceptionOnError = false)
  }

  /* Checks that the parse error position
   * corresponds to the syntax error position.
   *
   * Since the ddlParser falls back to the sqlParser
   * on error throwing the correct parse exception
   * is crucial. I.e., this test makes sure that not only
   * exception from the sqlParser is thrown on failure
   * but the one from the parser that consumed the most characters.
   */
  test("check reasonable parse errors") {

    val wrongSyntaxQueries = Array(
      ("CREATE TEMPORARY TABLE table001 (a1 int_, a2 int)", 1, 37),
      ("CREATE VIEW bla AZZ SELECT * FROM foo", 1, 17),
      ("""CREATE TEMPORARY TABL____ table001 (a1 int, a2 int)
USING com.sap.spark.velocity
OPTIONS (
  tableName "table001",
  hosts "localhost",
  local "true")""", 1, 18),
      ("""CREATE TEMPORARY TABLE table001 (a1 int, a2 int)
USIN_ com.sap.spark.velocity
OPTIONS (
  tableName "table001",
  hosts "localhost",
  local "true")""", 2, 1),
      ("""CREATE TEMPORARY TABLE tab01(a int)
USING com.sap.spark.velocity
OPTIONS (
  tableName "table001",
  hosts "localhost",
  local "true"    """, 6, 19),
      ("SELCT * FROM table001", 1, 1),
      ("CREAT TABLE table001(a1 int)", 1, 1),
      ("SELECT * FROM table001 WHERE HAVING a > 5", 1, 30),
      ("", 1, 1),
      ("   ", 1, 4),
      ("\n\n\n\n", 5, 1),
      ("abcdefg", 1, 1),
      ("SELECT SELECT", 1, 8)
    )

    for((query, line, col) <- wrongSyntaxQueries) {
      val vpe: VelocityParserException = intercept[VelocityParserException] {
        ddlParser.parse(query, ddlExceptionOnError = false)
      }
      val expLine = vpe.line
      val expCol = vpe.column
      assert(expLine == line)
      assert(expCol == col)
    }
  }
}

