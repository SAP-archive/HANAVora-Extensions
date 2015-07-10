package org.apache.spark.sql

import org.apache.spark.sql.sources.{ShowDatasourceTablesCommand, VelocityDDLParser}
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
}
