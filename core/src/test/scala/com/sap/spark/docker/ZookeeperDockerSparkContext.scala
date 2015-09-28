package com.sap.spark.docker

import com.spotify.docker.client.messages.ContainerInfo
import org.scalatest.Suite

/**
 * Adds ZooKeeper to a [[DockerSparkContext]].
 */
private[spark] trait ZookeeperDockerSparkContext extends DockerSparkContext {
  self: Suite =>

  private val DOCKER_ZOOKEEPER_CONTAINER = s"zookeeper_${dockerCluster.runId}"

  private val DOCKER_ZOOKEEPER_PORT = "2181"

  private val DOCKER_ZOOKEEPER_IMAGE = "jplock/zookeeper"

  private var _zookeeperContainer: ContainerInfo = _

  private def zookeeperIp = _zookeeperContainer.networkSettings().ipAddress()

  /**
   * Zookeeper URL to be used by extending classes.
   * @return
   */
  def zookeeperUrl: String = s"$zookeeperIp:$DOCKER_ZOOKEEPER_PORT"

  /**
   * Retrieves a ZooKeeper Docker container if exists, otherwise creates it with
   * the given configuration.
   *
   * @return Docker container IP
   */
  private def getOrCreateZooKeeperContainer(): ContainerInfo =
    dockerCluster.getOrCreateContainer(
      containerId = DOCKER_ZOOKEEPER_CONTAINER,
      image = DOCKER_ZOOKEEPER_IMAGE,
      portTests = Seq(DOCKER_ZOOKEEPER_PORT -> "tcp")
    )

  override private[docker] def getOrCreateOtherServices(): Unit = {
    super.getOrCreateOtherServices()
    _zookeeperContainer = getOrCreateZooKeeperContainer()
  }

}
