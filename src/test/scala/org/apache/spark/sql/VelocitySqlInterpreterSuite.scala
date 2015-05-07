package org.apache.spark.sql

import java.util
import java.util.Properties

import org.apache.zeppelin.display.{AngularObjectRegistry, GUI}
import org.apache.zeppelin.interpreter.{InterpreterContextRunner, InterpreterContext, InterpreterGroup, InterpreterResult}
import org.apache.zeppelin.spark.SparkInterpreter
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Person(name: String, number: Int)

class VelocitySqlInterpreterSuite extends FunSuite with BeforeAndAfterAll {

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
      velocityContext.sparkContext.parallelize(Seq(Person("moon", 1), Person("sun", 2))))
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
      new util.HashMap[String, Object](),
      new GUI,
      new AngularObjectRegistry(ig.getId, null),
      new util.LinkedList[InterpreterContextRunner]())
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
                   |USING corp.sap.spark.velocity
                   |OPTIONS (
                   |tableName "createTestTable",
                   |schema "name varchar(*), number integer",
                   |hosts "host",
                   |paths "path",
                   |localCatalog "true")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple create table with error") {

    val query = s"""CREATE TEMPORARY TABLE createTestTableWithError
                   |USING corp.sap.spark.velocity
                   |OPTIONS (
                   |tableName "createTestTableWithError",
                   |schema "name varchar(*), number integer",
                   |hosts "host1,host2",
                   |paths "path",
                   |localCatalog "true")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert("hosts and paths must have the same size" == ret.message())
  }

  test("Bad query") {
    val query = "BAD QUERY"

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert(
      "[1.1] failure: ``insert'' expected but identifier BAD found\n\nBAD QUERY\n^" == ret.message()
    )
  }
}