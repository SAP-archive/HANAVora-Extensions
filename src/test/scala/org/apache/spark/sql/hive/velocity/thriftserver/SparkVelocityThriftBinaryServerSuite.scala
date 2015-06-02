
package org.apache.spark.sql.hive.velocity.thriftserver

import java.io.File
import java.sql.{DriverManager, ResultSet, Statement}
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.spark.Logging
import org.apache.spark.sql.util.CsvGetter
import org.apache.spark.util.Utils
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Random, Try}

class SparkVelocityThriftBinaryServerSuite extends SparkVelocityThriftJdbcTest2 with Logging {
  override def mode: ServerMode.Value = ServerMode.binary

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement { statement =>
      val queries = Seq(
        s"""CREATE TEMPORARY TABLE $tableName
            |USING corp.sap.spark.velocity.test
            |OPTIONS (
            |tableName "$tableName",
            |schema "$schema",
            |hosts "0.0.0.1,0.0.0.2,0.0.0.3,0.0.0.4",
            |eagerLoad "false",
            |local "true",
            |paths "$stds1,$stds2,$stds3,$stds4")""".stripMargin)

      queries.foreach(statement.execute)
      logInfo("Test table is created.")
    }
  }

  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
    // Transport creation logics below mimics HiveConnection.createBinaryTransport
    val rawTransport = new TSocket("localhost", serverPort)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val protocol = new TBinaryProtocol(transport)
    val client = new ThriftCLIServiceClient(new Client(protocol))

    transport.open()
    try f(client) finally transport.close()
  }

  test("GetInfo Thrift API") {
    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")

      assertResult("Spark SQL", "Wrong GetInfo(CLI_DBMS_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME).getStringValue
      }

      assertResult("Spark SQL", "Wrong GetInfo(CLI_SERVER_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME).getStringValue
      }

      assertResult(true, "Spark version shouldn't be \"Unknown\"") {
        val version = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER).getStringValue
        logInfo(s"Spark version: $version")
        version != "Unknown"
      }
    }
  }

  def resultSetTolist(rs: ResultSet): List[(Any, Any)] = {

    val values = new ListBuffer[(Any, Any)]
    while (rs.next())
      values += new Tuple2(rs.getString(1), rs.getInt(2))
    values.toList
  }



  // scalastyle:off magic.number
  test("JDBC query execution") {
    withJdbcStatement { statement =>
      assertResult(14, "Row count mismatch") {
        val resultSet = statement.executeQuery( s"""SELECT COUNT(*) FROM $tableName""")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }
  // scalastyle:on magic.number
  // scalastyle:off magic.number
  test("Simple select query no params") {
    withJdbcStatement { statement =>
      val resultSet = statement.executeQuery( s"""SELECT * FROM $tableName""")

      // Checking result size: 6 + 6 + 2 + 0
      val results = resultSetTolist(resultSet)

      assert(results contains("hans", 10))
      assert(results contains("wurst", 20))
      assert(results contains("hans", 20))
      assert(results contains("wurst", 40))
      assert(results contains("hans", 30))
      assert(results contains("wurst", 15))

      // Checking second node data
      assert(results contains("peter", 10))
      assert(results contains("john", 20))
      assert(results contains("peter", 20))
      assert(results contains("john", 40))
      assert(results contains("peter", 30))
      assert(results contains("john", 15))

      // Checking third node data
      assert(results contains("keith", 10))
      assert(results contains("paul", 20))


    }
  }
  // scalastyle:on magic.number

}

object ServerMode extends Enumeration {
  val binary, http = Value
}

abstract class SparkVelocityThriftJdbcTest2 extends SparkVelocityThriftServer2Test {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  private def jdbcUri = if (mode == ServerMode.http) {
    s"""jdbc:hive2://localhost:$serverPort/
          |default?
          |hive.server2.transport.mode=http;
          |hive.server2.thrift.http.path=cliservice
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://localhost:$serverPort/"
  }

  def withMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  protected def withJdbcStatement(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(f)
  }
}

// scalastyle:off magic.number
abstract class SparkVelocityThriftServer2Test extends FunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode.Value

  private var listeningPort: Int = _

  protected def serverPort: Int = listeningPort

  private var warehousePath: File = _
  private var metastorePath: File = _

  private def metastoreJdbcUri = s"""jdbc:derby:;databaseName=$metastorePath;create=true"""

  private val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  private var process: Process = _

  protected def serverStartCommand(port: Int) = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    s"""java -cp ${sys.props("java.class.path")}
        |  -Xms512m -Xmx512m -XX:MaxPermSize=128m org.apache.spark.deploy.SparkSubmit --class
        |  org.apache.spark.sql.hive.thriftserver.SparkVelocityThriftServer spark-internal
        |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
        |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
        |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
        |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
        |  --hiveconf $portConf=$port
     """.stripMargin.split("\\s+").toSeq
  }

  val tableName = "mockedTable"
  val schema = "name varchar(200), age integer"

  val stds1 = CsvGetter.getFileFromClassPath("/thriftserverTest/distributedSimple.part1")
  val stds2 = CsvGetter.getFileFromClassPath("/thriftserverTest/distributedSimple.part2")
  val stds3 = CsvGetter.getFileFromClassPath("/thriftserverTest/distributedSimple.part3")
  val stds4 = CsvGetter.getFileFromClassPath("/thriftserverTest/distributedSimple.part4")

  var includedJars = Seq("/")

  private def startThriftServer(port: Int, attempt: Int): Unit = {
    warehousePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath = Utils.createTempDir()
    metastorePath.delete()

    val command = serverStartCommand(port)

    logInfo(s"Trying to start SparkVelocityThriftServer: port=$port, mode=$mode, attempt=$attempt")

    val env = Seq(
      // Disables SPARK_TESTING to exclude log4j.properties in test directories.
      "SPARK_TESTING" -> "0",
      // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be started
      // at a time, which is not Jenkins friendly.
      "SPARK_PID_DIR" -> pidDir.getCanonicalPath)

    val serverStarted = Promise[Unit]()

    process = Process(command, None, env: _*).run(ProcessLogger(
      (line: String) => {
        if (line.contains("ThriftBinaryCLIService listening on") ||
          line.contains("Started ThriftHttpCLIService in http")) {
          serverStarted.trySuccess(())
        } else if (line.contains("HiveServer2 is stopped")) {
          // This log line appears when the server fails to start and terminates gracefully (e.g.
          // because of port contention).
          serverStarted.tryFailure(new RuntimeException("Failed to start HiveThriftServer2"))
        }
      }))

    Await.result(serverStarted.future, 2.minute)
  }

  private def stopThriftServer(): Unit = {
    process.destroy()
    Thread.sleep(3.seconds.toMillis)

    warehousePath.delete()
    warehousePath = null

    metastorePath.delete()
    metastorePath = null
  }

  override protected def beforeAll(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)

    // Retries up to 3 times with different port numbers if the server fails to start
    (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
      started.orElse {
        listeningPort += 1
        stopThriftServer()
        Try(startThriftServer(listeningPort, attempt))
      }
    }.get

    logInfo(s"SparkVelocityThriftServer started successfully")
  }

  override protected def afterAll(): Unit = {
    stopThriftServer()
    logInfo("SparkVelocityThriftServer stopped")
  }
}
