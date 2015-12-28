package com.sap.spark.docker

import com.sap.spark.{WithSapSQLContext, WithSparkContext}
import com.spotify.docker.client.messages.ContainerInfo
import org.apache.spark.util.FixSparkUtils
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.Suite

/**
 * Trait to be extended by any class using a [[org.apache.spark.sql.AbstractSapSQLContext]]
 * in a Docker environment.
 */
private[spark] trait DockerSparkContext
  extends WithSparkContext
  with WithSapSQLContext
  with AddJars
  with Logging {
  self: Suite =>

  protected lazy val dockerCluster: DockerCluster = new DockerCluster()

  @transient private var _sc: SparkContext = _

  override def sc: SparkContext = _sc

  private var _sparkMaster: Option[ContainerInfo] = None

  def sparkMasterIp: Option[String] =
    _sparkMaster.map(_.networkSettings().ipAddress())

  override def sparkMaster: String =
    sparkMasterIp
    .map(ip => s"spark://$ip:7077")
    .getOrElse(s"local[$numberOfSparkWorkers]")

  @transient private var _sparkWorkers: Seq[ContainerInfo] = Seq()

  def hosts: Seq[String] = _sparkWorkers map { c => c.networkSettings().ipAddress() }

  val userHome = System.getProperty("user.home")

  /**
   * If true, the Spark master will be in Docker.
   * Otherwise, it will be local.
   *
   * @return
   */
  def useSparkInDocker: Boolean = false

  override def sparkConf: SparkConf = {
    val _sparkConf = super.sparkConf.clone()
    _sparkConf.set("spark.driver.host", dockerCluster.hostIp)
    _sparkConf
  }

  /** Ensure that Spark sees SPARK_LOCAL_HOST */
  private def fixSparkLocalhost(): Unit =
    FixSparkUtils.setCustomHostname(dockerCluster.hostIp)

  /**
   * Starts a docker containers to build a spark cluster.
   */
  private[docker] def startDockerSparkCluster(): Unit = {
    getOrCreateOtherServices()
    if (useSparkInDocker) {
      _sparkMaster = Option(getOrCreateSparkMaster())
    }
    _sparkWorkers = (1 to numberOfSparkWorkers).par.map { i =>
      getOrCreateSparkWorker(i)
    }.toList
  }

  /**
   * Implementations can override this to start other services
   * (e.g. ZooKeeper).
   */
  private[docker] def getOrCreateOtherServices(): Unit = { }

  /**
   * Implementations must override this to create a Spark mas
   * @return
   */
  private[docker] def getOrCreateSparkMaster(): ContainerInfo = {
    if (useSparkInDocker) {
      sys.error("getOrCreateSparkMaster() must be overriden if useSparkInDocker = true")
    }
    null
  }

  private[docker] def getOrCreateSparkWorker(i: Int): ContainerInfo

  override protected def setUpSparkContext(): Unit = {
    fixSparkLocalhost()
    startDockerSparkCluster()
    createSparkContext()
  }

  /**
   * Convenience method for starting a spark context
   */
  private def createSparkContext() {
    logDebug(s"Creating SparkContext with master $sparkMaster")
    _sc = new SparkContext(sparkMaster, "docker-spark-test", sparkConf)
    if (useSparkInDocker) {
      addClassPathToSparkContext()
    }
  }

  override protected def tearDownSparkContext(): Unit = {
    stopSparkContext()
    dockerCluster.shutdown()
  }

  /**
   * Convenience method for bringing down the Spark Context
   */
  private def stopSparkContext() = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    cleanUpCreatedJars()
  }

  /**
   * Restart the spark context to simulate a cluster restart
   */
  protected def restartSparkContext(): Unit = {
    stopSparkContext()
    createSparkContext()
  }

  protected def stopSparkWorker(i: Int): Unit = {
    val id = _sparkWorkers(i).id()
    logDebug(s"Stopping the Spark Worker $id: ${hosts(i)}")
    dockerCluster.docker.stopContainer(id, 10) // scalastyle:ignore
  }

  protected def restartSparkWorker(i: Int): Unit = {
    val id = _sparkWorkers(i).id()
    logDebug(s"Restarting the Spark Worker $id: ${hosts(i)}")
    dockerCluster.docker.startContainer(id)
  }

}
