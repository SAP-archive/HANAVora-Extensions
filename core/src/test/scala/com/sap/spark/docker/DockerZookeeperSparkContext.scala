package com.sap.spark.docker

import com.spotify.docker.client.messages.{Container, ContainerConfig, HostConfig}
import com.spotify.docker.client.{DockerClient, DockerException}
import org.scalatest.Suite

import scala.collection.JavaConverters._

/**
 * Generic trait to automatically start a Docker container with a Zookeeper instance. It
 * initializes a SparkContext.
 */
private[spark] trait DockerZookeeperSparkContext extends DockerSparkContext {
  self: Suite =>

  protected val DOCKER_ZOOKEEPER_CONTAINER = s"zookeeper_$RUN_ID"

  protected val DOCKER_ZOOKEEPER_PORT = "2181"

  protected val DOCKER_ZOOKEEPER_IMAGE = "jplock/zookeeper"

  lazy val zookeeperUrl: String = 
    s"${getContainerIp(DOCKER_ZOOKEEPER_CONTAINER)}:${DOCKER_ZOOKEEPER_PORT}"

  /**
   * Retrieves a zookeeper docker container if exists, otherwise creates it with 
   * the given configuration.
   * 
   * @return Docker container IP
   */
  protected def getOrCreateZooKeeperContainer: String =
    getOrCreateContainer(
      containerId = DOCKER_ZOOKEEPER_CONTAINER,
      containerConfig = ContainerConfig.builder()
        .image(DOCKER_ZOOKEEPER_IMAGE)
        .tty(true)
        .memory(dockerRamLimit)
        .hostConfig(HostConfig.builder()
        /* TODO: publishAllPorts(true) */
        .build()
        ).build(),
      portTests = Seq("2181" -> "tcp")
    )

  override protected def startDockerSparkCluster(): Unit = {
    time(s"Creating Zookeeper Container") {
      getOrCreateZooKeeperContainer
    }
    super.startDockerSparkCluster()
  }

  override protected def stopRemoveAllContainers(): Unit = {
    super.stopRemoveAllContainers
    
    if (docker == null) {
      docker = buildDockerClient()
    }
    val defaultTimeout = 1000
    var iterations = 0
    val maximumIterations = 10
    var containers: Seq[Container] = Seq()
    do {
      containers = docker
        .listContainers(DockerClient.ListContainersParam.allContainers(true))
        .asScala.toSeq
        .filter(_.names().asScala.exists({ name =>
        name.startsWith("/" + DOCKER_ZOOKEEPER_CONTAINER)
      }))
      containers.foreach { container =>
        try {
          docker.stopContainer(container.id(), 0)
          docker.removeContainer(container.id())
        } catch {
          case ex: DockerException =>
            logDebug(s"Cannot stop or remove container: ${ex.getMessage}")
            Thread sleep defaultTimeout
        }
      }
      iterations += 1
    } while (containers.nonEmpty && iterations < maximumIterations)
  }
}
