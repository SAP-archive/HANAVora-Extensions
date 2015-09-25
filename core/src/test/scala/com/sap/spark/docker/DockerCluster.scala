package com.sap.spark.docker

import java.io.IOException
import java.net._

import com.sap.spark.util.TestUtils._
import com.spotify.docker.client._
import com.spotify.docker.client.messages._
import org.apache.spark.Logging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Random, Success, Try}

private object DockerCluster {

  val RUN_ID_LENGTH = 5

  def newRunId: String = Random.alphanumeric.take(RUN_ID_LENGTH).mkString

}

/**
 * A [[DockerCluster]] represents a set of Docker containers to be used
 * together.
 *
 * Usage:
 *
 * {{{
 *   val dockerCluster = new DockerCluster()
 *   dockerCluster.getOrCreateContainer(containerId = "myContainer", image = "hello-world")
 *   dockerCluster.getOrCreateContainer(containerId = "myContainer2", image = "hello-world")
 *
 *   /* Use containers */
 *
 *   dockerCluster.shutdown()
 * }}}
 *
 * @param runId
 */
class DockerCluster(val runId: String = DockerCluster.newRunId) extends Logging {

  private var _docker: DockerClient = null

  /**
   * The underlying [[DockerClient]].
   *
   * @return
   */
  def docker: DockerClient = {
    if (_docker == null) {
      _docker = buildDockerClient()
    }
    _docker
  }

  private def buildDockerClient(): DockerClient = {
    DefaultDockerClient
      .fromEnv()
      .build()
  }

  private def closeDockerClient(): Unit = {
    if (_docker != null) {
      _docker.close()
      _docker = null
    }
  }

  /**
   * A memory limit (in bytes) for each container. Default to 1GB.
   */
  private lazy val ramLimit: Long =
    getSetting("docker.ram.limit", (1 * 1024 * 1024 * 1024).toString).toLong

  /**
   * If this is set to true (default), the Docker cluster will be clean
   * up before and after the tests.
   */
  private lazy val resetDockerCluster: Boolean =
    getSetting("docker.reset.cluster", "true").toBoolean

  val userHome = System.getProperty("user.home")

  private val containers: mutable.ListBuffer[String] = mutable.ListBuffer()

  /**
   * We assume that the Docker bridge interface is docker0 and
   * that we are using IPv4.
   */
  lazy val hostIp: String =
    NetworkInterface.getByName("docker0").getInetAddresses.asScala.toList
      .find(_.isInstanceOf[Inet4Address])
      .map(_.getHostAddress)
      .getOrElse(sys.error("docker0 interface not found"))

  /**
   * Closes the cluster. If docker.reset.cluster is true (default), all
   * containers started with this [[DockerCluster]] will be killed and
   * removed.
   */
  def shutdown(): Unit = {
    if (resetDockerCluster) {
      stopRemoveAllContainers()
    }
    closeDockerClient()
  }

  /**
   * Creates a container if it does not exist already and returns its [[ContainerInfo]].
   *
   * @param containerId Name for the container.
   * @param image Docker image to use.
   * @param env A list of environment variables in the form of key=val.
   * @param portTests A list of ports to test in the form of (port, protocol),
   *                  where protocol can be TCP or HTTP.
   * @return
   */
  def getOrCreateContainer(
                            containerId: String,
                            image: String,
                            env: Seq[String] = Nil,
                            cmd: Seq[String] = Nil,
                            binds: Seq[String] = Nil,
                            portTests: Seq[(String, String)] = Nil
                            ): ContainerInfo = {
    logDebug(s"Get or create: $containerId")
    val containerInfo = Try({
      docker inspectContainer containerId
    }) match {
      case Success(i) => i
      case Failure(_: ContainerNotFoundException) =>
        try {
          docker.inspectImage(image)
        } catch {
          case ex: ImageNotFoundException => pullImage(image)
        }
        logDebug(s"Create: $containerId")
        val containerConfig = buildContainerConf(containerId, image, env, cmd, binds)
        docker.createContainer(containerConfig, containerId)
        synchronized {
          if (!containers.contains(containerId)) {
            containers += containerId
          }
        }
        logDebug(s"Start: $containerId")
        docker.startContainer(containerId)
        docker inspectContainer containerId
      case Failure(ex) =>
        throw ex
    }
    val ip = containerInfo.networkSettings().ipAddress()

    Await.ready(
      Future.sequence(
        portTests.map({
          case (port, proto) => future {
            waitUntilNodeIsReady(ip, port, proto)
          }
        })
      ), Duration.Inf
    )

    containerInfo
  }

  private def buildContainerConf(containerId: String,
                                 image: String,
                                 env: Seq[String],
                                 cmd: Seq[String],
                                 binds: Seq[String]): ContainerConfig = {
    val hostConfigBuilder = HostConfig.builder()
      .dnsSearch(".")
    if (binds.nonEmpty) {
      hostConfigBuilder.binds(binds: _*)
    }
    val configBuilder = ContainerConfig.builder()
      .image(image)
      .memory(ramLimit)
      .tty(true)
      .hostConfig(hostConfigBuilder.build())
    if (env.nonEmpty) {
      configBuilder.env(env: _*)
    }
    if (cmd.nonEmpty) {
      configBuilder.cmd(cmd: _*)
    }
    configBuilder.build()
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

  private def stopRemoveAllContainers(): Unit = {
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

}
