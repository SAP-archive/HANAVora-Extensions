
package org.apache.spark.sql.hive.velocity.thriftserver


import java.io.File
import java.sql.{ResultSet, Date, DriverManager, Statement}

import corp.sap.spark.velocity.util.CsvGetter._
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.scheduler.StatsReportListener
import org.apache.spark.sql.hive.thriftserver.SparkSQLEnv
import org.apache.spark.sql.{VelocitySQLContext, Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, ArrayBuffer}
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Random, Try}

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.jdbc.HiveDriver
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import org.apache.spark.{SparkContext, SparkConf, Logging}
import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql.hive.{HiveContext, HiveShim}
import org.apache.spark.util.Utils


class SparkVelocityThriftBinaryServerSuite extends SparkVelocityThriftJdbcTest2 with Logging {
  override def mode = ServerMode2.binary

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


      //logInfo(s"""${sys.props("java.class.path")}""");
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



  //scalastyle:off magic.number
  test("JDBC query execution") {
    withJdbcStatement { statement =>
      assertResult(14, "Row count mismatch") {
        val resultSet = statement.executeQuery( s"""SELECT COUNT(*) FROM $tableName""")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }
  //scalastyle:on magic.number
  //scalastyle:off magic.number
  test("Simple select query no params") {
    withJdbcStatement { statement =>
      val resultSet = statement.executeQuery( s"""SELECT * FROM $tableName""")

      // Checking result size: 6 + 6 + 2 + 0
      //assertResult(14)(resultSetTolist(resultSet))
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
  //scalastyle:on magic.number

}

object ServerMode2 extends Enumeration {
  val binary, http = Value
}

abstract class SparkVelocityThriftJdbcTest2 extends SparkVelocityThriftServer2Test {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  private def jdbcUri = if (mode == ServerMode2.http) {
    s"""jdbc:hive2://localhost:$serverPort/
                                            |default?
                                            |hive.server2.transport.mode=http;
                                            |hive.server2.thrift.http.path=cliservice
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://localhost:$serverPort/"
  }

  protected def withJdbcStatement(f: Statement => Unit): Unit = {
    val connection = DriverManager.getConnection(jdbcUri, user, "")
    val statement = connection.createStatement()

    try f(statement) finally {
      statement.close()
      connection.close()
    }
  }
}

abstract class SparkVelocityThriftServer2Test extends FunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode2.Value

  private val CLASS_NAME = SparkVelocityThriftServer.getClass.getCanonicalName.stripSuffix("$")
  private val LOG_FILE_MARK = s"starting $CLASS_NAME, logging to "

  private val sbinDir =  s"${sys.props("sbinDir")}";
  private val startScript =
    s"$sbinDir/start-sparkvelocitythriftserver.sh".split("/").mkString(File.separator)
  private val stopScript =
    s"$sbinDir/stop-sparkvelocitythriftserver.sh".split("/").mkString(File.separator)

  private var listeningPort: Int = _

  protected def serverPort: Int = listeningPort

  protected def user = System.getProperty("user.name")

  private var warehousePath: File = _
  private var metastorePath: File = _

  private def metastoreJdbcUri = s"""jdbc:derby:;databaseName=$metastorePath;create=true"""

  private val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  private var logPath: File = _
  private var logTailingProcess: Process = _
  private var diagnosisBuffer: ArrayBuffer[String] = ArrayBuffer.empty[String]
  val defaultSparkHome = "~/hanalite-spark"


  val tableName = "mockedTable"
  val schema = "name varchar(200), age integer"

  val stds1 = getFileFromClassPath("/thriftserverTest/distributedSimple.part1")
  val stds2 = getFileFromClassPath("/thriftserverTest/distributedSimple.part2")
  val stds3 = getFileFromClassPath("/thriftserverTest/distributedSimple.part3")
  val stds4 = getFileFromClassPath("/thriftserverTest/distributedSimple.part4")

  var includedJars = Seq("/")




  private def startThriftServer(port: Int, attempt: Int) = {

    warehousePath = util.getTempFilePath("warehouse")
    metastorePath = util.getTempFilePath("metastorehivethriftserver")

    val portConf = if (mode == ServerMode2.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    val thriftServer = new SparkVelocityThriftServer

    val sparkConf = new SparkConf(loadDefaults = true)
    val maybeSerializer = sparkConf.getOption("spark.serializer")
    val maybeKryoReferenceTracking = sparkConf.getOption("spark.kryo.referenceTracking")

    sparkConf
      .setAppName(
        s"""SparkVelocityThriftServer::SparkSQL::
           |${java.net.InetAddress.getLocalHost.getHostName}""".stripMargin)
      .set("spark.sql.hive.version", HiveShim.version)
      .set(
        "spark.serializer",
        maybeSerializer.getOrElse("org.apache.spark.serializer.KryoSerializer"))
      .set(
        "spark.kryo.referenceTracking",
        maybeKryoReferenceTracking.getOrElse("false"))
      .set("spark.ui.enabled","false")
      .set("spark.driver.userClassPathFirst","true")
      .set("spark.executor.userClassPathFirst","true")
      .setMaster("local")

    sys.props.put(s"""${ConfVars.METASTORECONNECTURLKEY}""",s"""${metastoreJdbcUri}""")
    SparkSQLEnv.sparkContext = new SparkContext(sparkConf)
    SparkSQLEnv.sparkContext.addSparkListener(new StatsReportListener())
    SparkSQLEnv.hiveContext = new VelocitySQLContext(SparkSQLEnv.sparkContext){
      @transient override protected[hive] lazy val hiveconf: HiveConf = {
        setConf(sessionState.getConf.getAllProperties)
        sessionState.getConf
      }
      hiveconf.set("hive.root.logger","INFO,console")
      hiveconf.set(s"""${ConfVars.METASTORECONNECTURLKEY}""",s"""${metastoreJdbcUri}""")
      hiveconf.set(s"""${ConfVars.METASTOREWAREHOUSE}""",s"""${warehousePath}""")
      hiveconf.set(s"""${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}""","localhost")
      hiveconf.set(s"""${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}""",s"""${mode}""")
      hiveconf.set(s"""${portConf}""",s"""${port}""")


    }.asInstanceOf[HiveContext]


    thriftServer.startWithContext(SparkSQLEnv.hiveContext)



  }

  private def stopThriftServer(): Unit = {
    warehousePath.delete()
    metastorePath.delete
  }

  private def dumpLogs(): Unit = {
    logError(
      s"""
         |=====================================
         |SparkVelocityThriftServer2Suite failure output
         |=====================================
         |${diagnosisBuffer.mkString("\n")}
          |=========================================
          |End SparkVelocityThriftServer2Suite failure output
          |=========================================
       """.stripMargin)
  }

  //scalastyle:off magic.number
  override protected def beforeAll(): Unit = {
    // Chooses a random port between 10000 and 19999
    listeningPort = 10000 + Random.nextInt(10000)
    diagnosisBuffer.clear()
    startThriftServer(listeningPort,0)

    logInfo(s"SparkVelocityThriftServer2 started successfully")
    //wait for a few seconds as the listeningport gets ready
    Thread sleep 10000

  }
  //scalastyle:on magic.number

  override protected def afterAll(): Unit = {
    stopThriftServer()
    logInfo("SparkVelocityThriftServer2 stopped")
  }
}
