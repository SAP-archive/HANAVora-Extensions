package com.sap.spark.docker

import java.io.IOException
import java.net._

import com.sap.spark.util.TestUtils._
import com.sap.spark.{WithSapSQLContext, WithSparkContext}
import com.spotify.docker.client._
import com.spotify.docker.client.messages.{ContainerConfig, ProgressMessage}
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success, Try}

/**
 * Generic trait to be extended by any class/trait using a SapSQLContext in a docker
 * container. It contains generic methods to generate a docker container with the given
 * configuration.
 */
private[spark] trait DockerSparkContext
  extends WithSparkContext
  with WithSapSQLContext
  with Logging {
  self: Suite =>

  /**
   * The RUN ID identifies the set of Docker containers used for this test run. If none
   * is specified, a random one will be generated. If a Docker cluster needs to be re-used,
   * this must be set. This can also be set from Jenkins to have better control of which Docker
   * containers were used for a given Jenkins job.
   */
  protected lazy val RUN_ID =
    getSetting("docker.run.id",
      Random.alphanumeric.take(5).mkString // scalastyle:ignore
    )

  protected var docker: DockerClient = null

  /**
   * A memory limit (in bytes) for each container. Default to 1GB.
   */
  protected lazy val dockerRamLimit: Long =
    getSetting("docker.ram.limit", (1 * 1024 * 1024 * 1024).toString).toLong

  /**
   * If this is set to true (default), the Docker cluster will be clean
   * up before and after the tests.
   */
  protected lazy val resetDockerSparkCluster: Boolean =
    getSetting("docker.reset.cluster", "true").toBoolean

  @transient protected var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  @transient protected var _sparkWorkers: Seq[String] = Seq()

  def hosts: Seq[String] = _sparkWorkers

  val userHome = System.getProperty("user.home")

  protected var timingResults: Seq[String] = Seq()

  private val containers: mutable.ListBuffer[String] = mutable.ListBuffer()

  /**
   * We assume that the Docker bridge interface is docker0 and
   * that we are using IPv4.
   */
  protected lazy val dockerHostIp: String =
    NetworkInterface.getByName("docker0").getInetAddresses.asScala.toList
      .find(_.isInstanceOf[Inet4Address])
      .map(_.getHostAddress)
      .getOrElse(sys.error("docker0 interface not found"))

  override lazy val sparkConf: SparkConf = {
    val _sparkConf = super.sparkConf.clone()
    _sparkConf.set("spark.driver.host", dockerHostIp)
    _sparkConf
  }

  def cleanUpDockerCluster(): Unit = {
    if (resetDockerSparkCluster) {
      if (docker == null) {
        docker = buildDockerClient()
      }
      stopRemoveAllContainers()
      docker.close()
      docker = null
    }
  }

  /**
   * Starts a docker containers to build a spark cluster.
   */

  protected def startDockerSparkCluster(): Unit = {
    timingResults.foreach(msg => logDebug(msg))
  }

  protected def getOrCreateContainer(containerId: String,
                                     containerConfig: ContainerConfig,
                                     portTests: Seq[(String, String)] = Nil
                                      ): String = {
    if (docker == null) {
      docker = buildDockerClient()
    }
    logDebug(s"Get or create: $containerId")
    val ip = Try({
      getContainerIp(containerId)
    }) match {
      case Success(i) => i
      case Failure(_: ContainerNotFoundException) =>
        try {
          docker.inspectImage(containerConfig.image())
        } catch {
          case ex: ImageNotFoundException => pullImage(containerConfig.image())
        }
        logDebug(s"Create: $containerId")
        docker.createContainer(containerConfig, containerId)
        synchronized {
          if (!containers.contains(containerId)) {
            containers += containerId
          }
        }
        logDebug(s"Start: $containerId")
        docker.startContainer(containerId)
        getContainerIp(containerId)
      case Failure(ex) =>
        throw ex
    }

    Await.ready(
      Future.sequence(
        portTests.map({
          case (port, proto) => future {
            waitUntilNodeIsReady(ip, port, proto)
          }
        })
      ), Duration.Inf
    )

    ip
  }

  /**
   * Pulls a remote docker image by its name.
   *
   * @param image docker image name
   */
  private def pullImage(image: String): Unit = {
    logDebug(s"Pulling image: $image")
    docker.pull(image, new ProgressHandler {
      override def progress(message: ProgressMessage): Unit = {
        if (message.progress() != null) {
          logDebug(s"Pulling $image: ${message.progress()}")
        }
      }
    })
    logDebug(s"Pulled image: $image")
  }

  /**
   * Retrieves the container IP for a container id.
   *
   * @param containerId container id
   *
   * @return container IP
   */
  protected def getContainerIp(containerId: String): String =
    (docker inspectContainer containerId).networkSettings().ipAddress()

  /**
   * Stops and removes every container in the local machine
   *
   * a container cannot be removed without stopping
   * if a container did not stop before removing an exception occurs
   * this method waits and retries until every container is removed
   */
  protected def stopRemoveAllContainers(): Unit = {
    if (docker == null) {
      docker = buildDockerClient()
    }
    for (containerId <- containers) {
      val containerOpt = docker
        .listContainers(DockerClient.ListContainersParam.allContainers(true)).asScala
        .find(_.names().asScala.exists(_ == s"/$containerId"))
      containerOpt match {
        case Some(container) =>
          docker.killContainer(container.id())
          docker.removeContainer(container.id())
          containers -= containerId
        case None =>
          logWarning(s"Container $containerId not found, cannot remove it")
      }
    }
    if (containers.nonEmpty) {
      logError(s"Could not kill and remove all containers: ${containers.mkString(",")}")
    }
  }

  /**
   * Checks an http port to check if a http service is up
   *
   * it is used to check if a spark master or spark worker is up
   *
   * @param ip
   * @param port
   * @param proto
   */
  private def waitUntilNodeIsReady(ip: String, port: String, proto: String): Unit = {
    var isReady = false
    var iterations = 0
    val maximumIterations = 50
    val defaultTimeout = 1000
    do {
      logDebug(s"Pinging $ip:$port (iteration $iterations)")
      isReady = proto match {
        case "http" => pingHttp(ip, port, defaultTimeout)
        case "tcp" => pingTcp(ip, port, defaultTimeout)
        case "none" => true
      }
      if (!isReady) {
        Thread sleep defaultTimeout
      }
      iterations += 1
    } while (!isReady && iterations < maximumIterations)

    if (!isReady) {
      sys.error(s"Node $ip is not replying for $port/$proto")
    }
    logDebug(s"Ready: $ip:$port/$proto")
  }

  /**
   * Pings a HTTP URL.
   * This effectively sends a HEAD request and returns <code>true</code>
   * if the response code is in the 200-399 range.
   *
   * @param ip The IP or host to be pinged.
   * @param port HTTP port.
   * @param timeout The timeout in millis for both the connection timeout
   *                and the response read timeout. Note that
   *                the total timeout is effectively two times the given timeout.
   * @return <code>true</code> if the given HTTP URL has returned
   *         response code 200-399 on a HEAD request within the
   *         given timeout, otherwise <code>false</code>.
   */
  private def pingHttp(ip: String, port: String, timeout: Int): Boolean = {
    val url = s"http://$ip:$port"
    try {
      val tempConnection = new URL(url).openConnection()
      val connection = tempConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(timeout)
      connection.setReadTimeout(timeout)
      connection.setRequestMethod("HEAD")
      val responseCode = connection.getResponseCode
      200 <= responseCode && responseCode <= 399
    } catch {
      case ioe: IOException => false
    }
  }

  /**
   * Pings to a TCP port.
   *
   * @param ip Machine IP
   * @param port Machine port
   * @param timeout Maximum timeout
   *
   * @return true, if address responds; false, otherwise.
   */
  private def pingTcp(ip: String, port: String, timeout: Int): Boolean = {
    Try({
      val socket = new Socket(ip, port.toInt)
      socket.close()
    }) match {
      case Success(_) => true
      case Failure(ex: ConnectException) =>
        logTrace(s"Error when testing connection to $ip:$port", ex)
        false
      case Failure(ex) =>
        logDebug(s"Error when testing connection to $ip:$port", ex)
        false
    }
  }

  /**
   * Builds a Docker client.
   */
  protected def buildDockerClient(): DockerClient = {
    DefaultDockerClient
      .fromEnv()
      .build()
  }

  /**
   * Time measurement function
   */
  protected def time[A](id: String)(f: => A) = {
    val s = System.nanoTime
    val ret = f
    val result = (System.nanoTime - s) / 1e6
    timingResults = timingResults :+ s"time in $id: $result ms. Result: $ret"
    ret
  }

  override protected def tearDownSparkContext(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    cleanUpDockerCluster()
  }

}
