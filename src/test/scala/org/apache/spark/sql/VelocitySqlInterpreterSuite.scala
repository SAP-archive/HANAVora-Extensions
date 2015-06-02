package org.apache.spark.sql

import java.util
import java.util.Properties

import org.apache.zeppelin.display.{AngularObjectRegistry, GUI}
import org.apache.zeppelin.interpreter.{InterpreterContextRunner, InterpreterContext, InterpreterGroup, InterpreterResult}
import org.apache.zeppelin.spark.SparkInterpreter
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Person(name: String, number: Int, pred: Int)

class VelocitySqlInterpreterSuite extends FunSuite with BeforeAndAfterAll {

  // scalastyle:off magic.number

  var sqli: VelocitySqlInterpreter = _
  var si: SparkInterpreter = _
  var context: InterpreterContext = _

  var velocityContext: VelocitySQLContext = null

  override protected def beforeAll() = {
    val p = new Properties
    p.put("spark.ui.enabled", "false")

    si = new SparkInterpreter(p)
    si.open()

    velocityContext = new VelocitySQLContext(si.getSparkContext)

    val df = velocityContext.createDataFrame(
      velocityContext.sparkContext.parallelize(
        Seq(
          Person("sun", 1, 0),
          Person("mercury", 2, 1),
          Person("venus", 3, 1),
          Person("earth", 4, 1),
          Person("jupiter", 5, 1),
          Person("saturn", 6, 1),
          Person("moon", 10, 4)
        )))
    velocityContext.registerDataFrameAsTable(df, "testTable")

    sqli = new VelocitySqlInterpreter(velocityContext)

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
                   |USING corp.sap.spark.velocity.test
                   |OPTIONS (
                   |tableName "createTestTable",
                   |schema "name varchar(*), number integer",
                   |hosts "host",
                   |paths "path",
                   |eagerLoad "false",
                   |local "true")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple create table with error") {

    val query = s"""CREATE TEMPORARY TABLE createTestTableWithError
                   |USING corp.sap.spark.velocity.test
                   |OPTIONS (
                   |schema "name varchar(*), number integer",
                   |hosts "host1,host2",
                   |paths "path",
                   |eagerLoad "false",
                   |local "true")""".stripMargin

    val ret = sqli.interpret(query, context)

    assertResult(InterpreterResult.Code.ERROR)(ret.code())
    assertResult("tablename is mandatory")(ret.message())
  }

  test("Bad query") {
    val query = "BAD QUERY"

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert(ret.message().contains("expected but identifier BAD found")
    )
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