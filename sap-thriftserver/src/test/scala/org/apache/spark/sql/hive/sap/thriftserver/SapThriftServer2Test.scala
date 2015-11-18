package org.apache.spark.sql.hive.sap.thriftserver


import java.io.File
import com.sap.spark.util.TestUtils._
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.Logging
import org.apache.spark.util.Utils
import scala.concurrent.duration._

import scala.concurrent.{Await, Promise}
import scala.sys.process.{Process, ProcessLogger}
import scala.util.{Try, Random}

/**
 * This class provides a standalone version of a running ThriftServer for testing purposes
 *
 */
class SapThriftServer2Test(val master: String = "local",
                            val classPath: String = sys.props("java.class.path"),
                            val mode: ServerMode.Value = ServerMode.binary,
                            val bindHost: String = "localhost",
                            val applicationJar: Option[String] = None,
                            val additionalJars: Option[String] = None
                          )
  extends Logging {

  private var listeningPort: Int = _
  private val standardPort = 10000

  protected def serverPort: Int = listeningPort

  private var warehousePath: File = _
  private var metastorePath: File = _

  private def metastoreJdbcUri = s"""jdbc:derby:;databaseName=$metastorePath;create=true"""

  private lazy val pidDir: File = Utils.createTempDir(namePrefix = "thriftserver-pid")
  // if process == null no thriftserver is running!
  private var process: Option[Process] = None

  // we need to set our own log4j.properties, to be able to determine the start of the server
  private val log4jpropFile = getFileFromClassPath("/log4j.thriftservertest.properties")

  private def serverStartCommand(port: Int) = {
    val portConf = if (mode == ServerMode.binary) {
      ConfVars.HIVE_SERVER2_THRIFT_PORT
    } else {
      ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT
    }

    val additionalJarsParam = additionalJars match {
      case None => ""
      case Some(s) => s"--jars $s"
    }

    val applicationJarParam = applicationJar match {
      case None => ""
      case Some(s) => s
    }

    /*

    This test should only start a local thriftserver during unit
    tests (consequently: client and local) for more information on that parameters refer to the
    Spark submit documentiation
    */
    s"""java -cp "$classPath"
        |  -Dlog4j.configuration="$log4jpropFile"
        |  -Xms512m -Xmx512m -XX:MaxPermSize=128m org.apache.spark.deploy.SparkSubmit
        |  --deploy-mode client
        |  --master $master
        |   $additionalJarsParam
        |  --class org.apache.spark.sql.hive.thriftserver.SapThriftServer
        |  $applicationJarParam
        |  spark-internal
        |  --hiveconf ${ConfVars.METASTORECONNECTURLKEY}=$metastoreJdbcUri
        |  --hiveconf ${ConfVars.METASTOREWAREHOUSE}=$warehousePath
        |  --hiveconf ${ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST}=$bindHost
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
    logInfo(s"Start command: $command")

    val env = Seq(
      // Disables SPARK_TESTING to exclude log4j.properties in test directories.
      "SPARK_TESTING" -> "0",
      // Points SPARK_PID_DIR to SPARK_HOME, otherwise only 1 Thrift server instance can be started
      // at a time, which is not Jenkins friendly.
      "SPARK_PID_DIR" -> pidDir.getCanonicalPath)

    val serverStarted = Promise[Unit]()

    process = Some(Process(command, None, env: _*).run(ProcessLogger(
      (stdout: String) => {
        logInfo(s"[SAPThriftServer] $stdout")
        if (stdout.contains("ThriftBinaryCLIService listening on") ||
          stdout.contains("Started ThriftHttpCLIService in http")) {
          serverStarted.trySuccess(())
        } else if (stdout.contains("HiveServer2 is stopped")) {
          // This log line appears when the server fails to start and terminates gracefully (e.g.
          // because of port contention).
          serverStarted.tryFailure(new RuntimeException("Failed to start HiveThriftServer2"))
        }
      })))

    Await.result(serverStarted.future, 2.minute)
  }

  /**
   * Stops a running thriftserver
   */
  def stopThriftServer(): Unit = {
    if(isRunning()) {
        val p = process.get
        p.destroy()
        Thread.sleep(3.seconds.toMillis)

        warehousePath.delete()
        warehousePath = null

        metastorePath.delete()
        metastorePath = null
        process = None
        logInfo("SparkSapThriftServer stopped")
      } else {
        logInfo("No SparkSapThriftserver running, no need to stop it!")
      }
    }

  /**
   * Start the thriftserver, port etc. are configured in the constructor
   */
  def startThriftServer(): Unit = {
    if(!isRunning()){

      // Chooses a random port between [standardPort, 2*standardPort - 1] and
      listeningPort = standardPort + Random.nextInt(standardPort)

      // Retries up to 3 times with different port numbers if the server fails to start
      (1 to 3).foldLeft(Try(startThriftServer(listeningPort, 0))) { case (started, attempt) =>
        started.orElse {
          listeningPort += 1
          stopThriftServer()
          Try(startThriftServer(listeningPort, attempt))
        }
      }.get

      logInfo(s"SparkSapThriftServer started successfully")
    } else {
      logInfo(s"SparkSapThriftServer is already running, do not start again!")
    }
  }

  def getServerAdress(): String = bindHost
  def getServerPort(): Int = listeningPort

  def getServerAdressAndPort(): String = getServerAdress() + ":" + getServerPort()

  def isRunning(): Boolean = process match {
    case None => false
    case Some(_) => true
  }

}

object ServerMode extends Enumeration {
  val binary, http = Value
}
