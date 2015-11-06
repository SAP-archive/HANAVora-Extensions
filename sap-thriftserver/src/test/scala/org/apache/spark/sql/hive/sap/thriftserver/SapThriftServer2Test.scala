package org.apache.spark.sql.hive.sap.thriftserver

import java.io.File

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Try, Random}
import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
/**
 * This class is a variant of the thriftserver that can be used for testing purposes. It starts
 * a separate thriftserver process. This class has to be extended and is by different flavors
 * of JDBC drivers (See SapThriftJdbcTest in the same package as an example).
 */
// scalastyle:off magic.number
abstract class SapThriftServer2Test(val master: String)
  extends FunSuite with BeforeAndAfterAll with Logging {
  def mode: ServerMode.Value

  private var listeningPort: Int = _

  protected def serverPort: Int = listeningPort

  private var warehousePath: File = _
  private var metastorePath: File = _

  private def metastoreJdbcUri = s"""jdbc:derby:;databaseName=$metastorePath;create=true"""

  private lazy val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  private var process: Process = _

  protected def serverStartCommand(port: Int) = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }
    /*

    This test should only start a local thriftserver during unit
    tests (consequently: client and local) for more information on that parameters refer to the
    Spark submit documentiation
    */
    s"""java -cp ${sys.props("java.class.path")}
        |  -Xms512m -Xmx512m -XX:MaxPermSize=128m org.apache.spark.deploy.SparkSubmit
        |  --deploy-mode client
        |  --master ${master}
        |  --class
        |  org.apache.spark.sql.hive.thriftserver.SapThriftServer spark-internal
        |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
        |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
        |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=localhost
        |  --hiveconf ${ConfVars.HIVE_SERVER2_TRANSPORT_MODE}=$mode
        |  --hiveconf $portConf=$port
     """.stripMargin.split("\\s+").toSeq
  }

  var includedJars = Seq("/")

  private def startThriftServer(port: Int, attempt: Int): Unit = {
    warehousePath = Utils.createTempDir()
    warehousePath.delete()
    metastorePath = Utils.createTempDir()
    metastorePath.delete()

    val command = serverStartCommand(port)

    logInfo(s"Trying to start SapThriftServer: port=$port, mode=$mode, attempt=$attempt")

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

    logInfo(s"SparkSapThriftServer started successfully")
  }

  override protected def afterAll(): Unit = {
    stopThriftServer()
    logInfo("SparkSapThriftServer stopped")
  }
}
