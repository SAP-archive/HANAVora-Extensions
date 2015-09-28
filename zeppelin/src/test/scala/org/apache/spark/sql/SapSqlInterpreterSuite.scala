package org.apache.spark.sql

import com.sap.spark.util.TestUtils._
import java.util.Properties
import org.apache.zeppelin.display.{AngularObjectRegistry, GUI}
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterContextRunner, InterpreterGroup, InterpreterResult}
import org.apache.zeppelin.spark.SparkInterpreter
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Person(name: String, number: Int, pred: Int)

class SapSqlInterpreterSuite extends FunSuite with BeforeAndAfterAll {

  // scalastyle:off magic.number

  var sqli: SapSqlInterpreter = _
  var si: SparkInterpreter = _
  var context: InterpreterContext = _

  var sapContext: SapSQLContext = null

  val filePath = getFileFromClassPath("/simpleData.json")

  override protected def beforeAll() = {
    val p = new Properties
    p.put("spark.ui.enabled", "false")

    si = new SparkInterpreter(p)
    si.open()

    sapContext = new SapSQLContext(si.getSparkContext)

    val df = sapContext.createDataFrame(
      sapContext.sparkContext.parallelize(
        Seq(
          Person("sun", 1, 0),
          Person("mercury", 2, 1),
          Person("venus", 3, 1),
          Person("earth", 4, 1),
          Person("jupiter", 5, 1),
          Person("saturn", 6, 1),
          Person("moon", 10, 4)
        )))
    sapContext.registerDataFrameAsTable(df, "testTable")

    sqli = new SapSqlInterpreter(new SapSqlContextProviderMock(sapContext))

    val ig = new InterpreterGroup
    ig.add(si)
    ig.add(sqli)

    sqli.setInterpreterGroup(ig)
    sqli.open()

    context = new InterpreterContext(
      "id",
      "title",
      "text",
      new java.util.HashMap[String, Object](),
      new GUI,
      new AngularObjectRegistry(ig.getId, null),
      new java.util.LinkedList[InterpreterContextRunner]())
  }

  test("Simple Select using JSON DataSource") {

    val filePath = getFileFromClassPath("/simpleData.json")

    val createQuery = s"""CREATE TEMPORARY TABLE createTestTable
                         |USING org.apache.spark.sql.json
                         |OPTIONS (path "$filePath")""".stripMargin

    val selectQuery = "select * from createTestTable"

    val createRet = sqli.interpret(createQuery, context)

    assert(InterpreterResult.Code.SUCCESS == createRet.code())

    val ret = sqli.interpret(selectQuery, context)

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple Select") {

    val query = "select * from testTable"

    val ret = sqli.interpret(query, context)

    ret.message()

    assert(ret.message() contains "name")
    assert(ret.message() contains "number")
    assert(ret.message() contains "moon")
    assert(ret.message() contains "sun")
    assert(ret.message() contains "1")
    assert(ret.message() contains "2")

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple create table") {

    val query = s"""CREATE TEMPORARY TABLE createTestTable
                   |USING org.apache.spark.sql.json
                   |OPTIONS (path "$filePath")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple create table with error") {

    val query = s"""CREATE TEMPORARY TABLE createTestTableFail
                   |USING org.apache.spark.sql.json
                   |OPTIONS (path "bad/path/file.json")""".stripMargin

    val ret = sqli.interpret(query, context)

    assertResult(InterpreterResult.Code.ERROR)(ret.code())
    assert(ret.message().contains("Input path does not exist"))
  }

  test("Bad query") {
    val query = "BAD QUERY"

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert(ret.message().contains("Syntax error"))
  }

  test("Tree view keyword") {
    val query = "treeview number pred name select * from testTable"

    val ret = sqli.interpret(query, context)

    ret.message()

    assert(ret.message() contains "\"name\":\"moon\"")
    assert(ret.message() contains "\"name\":\"earth\"")
    assert(ret.message() contains "\"name\":\"sun\"")

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Tree view node title changed") {
    val query = "treeview number pred number select * from testTable"

    val ret = sqli.interpret(query, context)

    ret.message()

    assert(ret.message() contains "\"name\":\"10\"") // moon
    assert(ret.message() contains "\"name\":\"4\"") // earth
    assert(ret.message() contains "\"name\":\"1\"") // sun

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Tree view missing parameter exception") {
    val query = "treeview"

    val ret = sqli.interpret(query, context)

    ret.message()

    assert(ret.message() contains "id column, pred column, name column can not be empty")

    assert(InterpreterResult.Code.ERROR == ret.code())
  }

  test("Tree view using same paramter for id and pred columns") {
    val query = "treeview number number name select * from testTable"

    val ret = sqli.interpret(query, context)

    ret.message()

    assert(ret.message() contains "id column, pred column can should be different")

    assert(InterpreterResult.Code.ERROR == ret.code())
  }
}