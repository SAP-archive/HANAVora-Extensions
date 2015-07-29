package org.apache.spark.sql.sources

import org.apache.spark.sql.VelocitySqlParser
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FunSuite, GivenWhenThen}

class VelocityDDLParserSuite extends FunSuite with TableDrivenPropertyChecks with GivenWhenThen {

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
        intercept[RuntimeException] {
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
}

