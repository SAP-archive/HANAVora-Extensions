package org.apache.spark.sql

import java.util
import java.util.Properties

import com.nflabs.zeppelin.display.GUI
import com.nflabs.zeppelin.interpreter.{InterpreterContext, InterpreterGroup, InterpreterResult}
import com.nflabs.zeppelin.spark.SparkInterpreter
import org.scalatest.{BeforeAndAfterAll, FunSuite}

case class Person(name: String, number: Int)

class VelocitySqlInterpreterSuite extends FunSuite with BeforeAndAfterAll {

  var sqli: VelocitySqlInterpreter = null
  var si: SparkInterpreter = null
  val context: InterpreterContext =
    new InterpreterContext("id", "title", "text",
      new util.HashMap[String, Object](), new GUI())

  var velocityContext: VelocitySQLContext = null

  override protected def beforeAll() = {
    val p = new Properties
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
                   |paths "path")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.SUCCESS == ret.code())
  }

  test("Simple create table with error") {

    val query = s"""CREATE TEMPORARY TABLE createTestTable
                   |USING corp.sap.spark.velocity
                   |OPTIONS (
                   |tableName "createTestTable",
                   |schema "name varchar(*), number integer",
                   |hosts "host1,host2",
                   |paths "path")""".stripMargin

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert("Hosts and Paths parameters must have the same size" == ret.message())
  }

  test("Bad query") {
    val query = "BAD QUERY"

    val ret = sqli.interpret(query, context)

    assert(InterpreterResult.Code.ERROR == ret.code())
    assert("[1.1] failure: ``insert'' expected but identifier BAD found\n\nBAD QUERY\n^" == ret.message())
  }
}