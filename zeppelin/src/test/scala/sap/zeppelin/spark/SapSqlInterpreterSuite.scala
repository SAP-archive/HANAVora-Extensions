package sap.zeppelin.spark

import java.util.Properties

import org.apache.zeppelin.display.{AngularObjectRegistry, GUI}
import org.apache.zeppelin.interpreter.{InterpreterContext, InterpreterGroup, InterpreterOutput, InterpreterResult, _}
import org.apache.zeppelin.spark.SparkInterpreter
import org.apache.zeppelin.user.AuthenticationInfo
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class SapSqlInterpreterSuite extends FunSuite
  with BeforeAndAfterAll{

  private val planetTableName: String = "planets"
  private var sql: SapSqlInterpreter = null
  private var repl: SparkInterpreter = null
  private var context: InterpreterContext = null
  private var intpGroup: InterpreterGroup = null

  def getSparkTestProperties: Properties = {
    val p: Properties = new Properties
    p.setProperty("master", "local[*]")
    p.setProperty("spark.app.name", "Zeppelin Test")
    p.setProperty("zeppelin.spark.maxResult", "1000")
    p.setProperty("zeppelin.spark.importImplicit", "true")
    p.setProperty("zeppelin.spark.useHiveContext", "false")
    p
  }

  override def beforeAll {
    val p: Properties = new Properties
    p.putAll(getSparkTestProperties)
    p.setProperty("zeppelin.spark.maxResult", "1000")
    p.setProperty("zeppelin.spark.concurrentSQL", "false")
    p.setProperty("zeppelin.spark.sql.stacktrace", "false")
    if (repl == null) {
      repl = new SparkInterpreter(p)
      intpGroup = new InterpreterGroup
      repl.setInterpreterGroup(intpGroup)
      repl.open
      sql = new SapSqlInterpreter(p)
      intpGroup = new InterpreterGroup
      intpGroup.put("note", ListBuffer[Interpreter]().asJava)
      intpGroup.get("note").add(repl)
      intpGroup.get("note").add(sql)
      sql.setInterpreterGroup(intpGroup)
      sql.open
    }
    context = new InterpreterContext("note", "id", "title", "text",
      new AuthenticationInfo,
      Map[String, AnyRef]().asJava,
      new GUI,
      new AngularObjectRegistry(intpGroup.getId, null),
      null,
      new ListBuffer[InterpreterContextRunner]().asJava,
      new InterpreterOutput(new InterpreterOutputListener() {
        def onAppend(out: InterpreterOutput, line: Array[Byte]) { }

        def onUpdate(out: InterpreterOutput, output: Array[Byte]) { }
    }))
  }


  test("Simple Select Json Test") {
    val filePath = this.getClass.getResource("/simpleData.json").getFile
    val createQuery =
      s"""CREATE TEMPORARY TABLE createTestTable USING org.apache.spark.sql.json
         |OPTIONS (path "$filePath")""".stripMargin
    val selectQuery = "select * from createTestTable"
    val createRet: InterpreterResult = sql.interpret(createQuery, context)
    assert(InterpreterResult.Code.SUCCESS eq createRet.code)
    val ret: InterpreterResult = sql.interpret(selectQuery, context)
    assert(InterpreterResult.Code.SUCCESS eq ret.code)
    assert(ret.message == "age\tname\n10\thans\n20\tpeter\n20\thans\n40\tpeter\n")
  }

  test("Use Invalid Query"){
    val result: InterpreterResult = sql.interpret("select with wrong syntax", context)

    assert((InterpreterResult.Code.ERROR eq result.code))
  }
}
