package corp.sap.spark

import java.io._
import java.net._
import java.util
import java.util.{Locale, UUID}
import java.util.jar.{Attributes, JarEntry, JarOutputStream}

import com.spotify.docker.client._
import com.spotify.docker.client.messages._
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hdfs.DFSClient
import org.apache.hadoop.hdfs.protocol.HdfsConstants
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.Suite

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Random, Failure, Success, Try}

/**
 * Shares a `SparkContext` connected to Docker initiated Cluster
 *  between all tests in a suite and closes it at the end
 */
trait DockerSparkContext extends WithSparkContext with Logging {
  self: Suite =>
  
  /**
   * The RUN ID identifies the set of Docker containers used for this test run. If none
   * is specified, a random one will be generated. If a Docker cluster needs to be re-used,
   * this must be set. This can also be set from Jenkins to have better control of which Docker
   * containers were used for a given Jenkins job.
   */
  private lazy val RUN_ID =
    getSetting("docker.run.id",
      Random.alphanumeric.take(5).mkString // scalastyle:ignore
    )

  private val DOCKER_SPARK_VELOCITY_CONTAINER = s"velocity_spark_$RUN_ID"
  private val DOCKER_ZOOKEEPER_CONTAINER = s"velocity_zookeeper_$RUN_ID"
  private val DOCKER_HDFS_CONTAINER = DOCKER_SPARK_VELOCITY_CONTAINER

  /**
   * This is the Docker image to be used for Velocity. This is expected to contain Spark master
   * and workers, HDFS namenode and datanodes, and Velocity.
   */
  private lazy val DOCKER_SPARK_VELOCITY_IMAGE =
    getSetting("docker.image.velocity", "sparkvelocity")

  private val DOCKER_ZOOKEEPER_IMAGE = "jplock/zookeeper"

  /**
   * A memory limit (in bytes) for each container. Default to 1GB.
   */
  protected lazy val dockerRamLimit: Long =
    getSetting("docker.ram.limit", (1*1024*1024*1024).toString).toLong

  /**
   * If this is set to true (default), the Docker cluster will be clean
   * up before and after the tests.
   */
  protected lazy val resetDockerSparkCluster: Boolean =
    getSetting("docker.reset.cluster", "true").toBoolean

  /**
   * Set to true (default) to start Docker cluster inside Docker. Otherwise,
   * a local Spark cluster will be used for the tests, and Docker containers will
   * be used only for ZooKeeper, HDFS and Velocity.
   */
  protected lazy val useSparkInDocker: Boolean =
    getSetting("use.spark.in.docker", "true").toBoolean

  @transient private var _sc: SparkContext = _
  override def sc: SparkContext = _sc

  override protected def setUpSparkContext(): Unit = {
    cleanUpDockerCluster()

    /* Set HADOOP_USER_NAME so that we do not get HDFS permission errors */
    System.setProperty("HADOOP_USER_NAME", "root")

    startDockerSparkCluster()
    logDebug(s"Creating SparkContext with master $sparkMaster")
    if (useSparkInDocker) {
      _sc = new SparkContext(s"spark://$sparkMaster", "docker-spark-test", sparkConf)
      addClassPathToSparkContext()
      checkIfHdfsIsWritable()
    } else {
      _sc = new SparkContext(s"local[$numberOfSparkWorkers]", "docker-spark-test", sparkConf)
    }
  }

  override protected def tearDownSparkContext(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
    _createdJars.foreach( path => {
      try {
        new File(path).delete()
      } catch {
        case ioe: IOException => logError(ioe.getMessage)
      }
    })
    cleanUpDockerCluster()
  }


  private var docker: DockerClient = _

  @transient private var _sparkWorkers: Seq[String] = Seq()
  def hosts: Seq[String] = _sparkWorkers

  lazy val zookeeperUrl: String = s"${getContainerIp(DOCKER_ZOOKEEPER_CONTAINER)}:2181"

  lazy val sparkMaster: String = s"${getContainerIp(DOCKER_SPARK_VELOCITY_CONTAINER)}:7077"

  lazy val hdfsNameNode: String = s"${getContainerIp(DOCKER_HDFS_CONTAINER)}:9000"

  val userHome = System.getProperty("user.home")

  @transient private var timingResults: Seq[String] = Seq()

  override def sparkConf: SparkConf = {
    super.sparkConf
    /* TODO: conf.set("spark.eventLog.enabled", "true") */
    /* TODO: Spark might pick an incorrect interface to announce the Driver */
    /* conf.set("spark.driver.host", "") */
  }

  override def afterAll() {
    try {
      super.afterAll()
    } finally {
      _createdJars.foreach( path => {
        try {
          new File(path).delete()
        } catch {
          case ioe: IOException => logError(ioe.getMessage)
        }
      })
    }
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
  private def startDockerSparkCluster(): Unit = {
    time(s"Clean up docker cluster") {
      cleanUpDockerCluster()
    }
    time(s"Creating Zookeeper Container") {
      getOrCreateZooKeeperContainer
    }
    time(s"Creating Spark Master") {
      getOrCreateSparkMaster
    }
    time(s"Creating $numberOfSparkWorkers Spark Workers") {
      _sparkWorkers = (1 to numberOfSparkWorkers).par.map { i =>
        getOrCreateSparkWorker(i)
      }.toList
    }
    timingResults.foreach(msg => logDebug(msg))
  }

  private def getOrCreateContainer(containerId: String,
                                   containerConfig: ContainerConfig,
                                   portTests: Seq[(String,String)] = Nil
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
        logDebug(s"Start: $containerId")
        this.synchronized {
          docker.startContainer(containerId)
        }
        getContainerIp(containerId)
      case Failure(ex) =>
        throw ex
    }

    Await.ready(
      Future.sequence(
        portTests.map({
          case (port, proto) => future { waitUntilNodeIsReady(ip, port, proto) }
        })
      ), Duration.Inf
    )

    ip
  }

  private def getOrCreateZooKeeperContainer: String =
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

  private def getOrCreateSparkMaster: String =
    getOrCreateContainer(
      containerId = DOCKER_SPARK_VELOCITY_CONTAINER,
      containerConfig = ContainerConfig.builder()
        .image(DOCKER_SPARK_VELOCITY_IMAGE)
        .tty(true)
        .cmd("/start-master.sh")
        .memory(dockerRamLimit)
        .hostConfig(HostConfig.builder()
        /* TODO: publishAllPorts(true) */
        .build()
        ).build(),
      portTests = Seq(
        "8080" -> "http", /* Spark Master UI */
        "9000" -> "tcp"  /* HDFS NameNode */
      )
    )

  private def getOrCreateSparkWorker(i: Int): String =
    getOrCreateContainer(
      containerId = s"${DOCKER_SPARK_VELOCITY_CONTAINER}_$i",
      containerConfig = ContainerConfig.builder()
        .image(DOCKER_SPARK_VELOCITY_IMAGE)
        .tty(true)
        .cmd("/start-worker.sh")
        .memory(dockerRamLimit)
        .hostConfig(HostConfig.builder()
        /* TODO: publishAllPorts(true) */
        .links(s"$DOCKER_SPARK_VELOCITY_CONTAINER:spark_master")
        .build()
        ).build(),
      portTests = Seq(
        "8081" -> "http", /* Spark Worker UI */
        "2202" -> "tcp"   /* v2server */
      )
    )

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

  private def getContainerIp(containerId: String): String =
    (docker inspectContainer containerId).networkSettings().ipAddress()

  /**
   * Stops and removes every container in the local machine
   *
   * a container cannot be rmeoved without stoppping
   * if a container did not stop before removing an exception occurs
   * this method waits and retries until every container is removed
   */
  private def stopRemoveAllContainers() {
    if (docker == null) {
      docker = buildDockerClient()
    }
    var iterations = 0
    val maximumIterations = 10
    val defaultTimeout = 1000
    var containers: Seq[Container] = Seq()
    do {
      containers = docker
        .listContainers(DockerClient.ListContainersParam.allContainers(true))
        .asScala.toSeq
        .filter(_.names().asScala.exists({ name =>
          name.startsWith("/" + DOCKER_SPARK_VELOCITY_CONTAINER) ||
          name.startsWith("/" + DOCKER_ZOOKEEPER_CONTAINER) ||
            name.startsWith("/" + DOCKER_HDFS_CONTAINER)
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
      if(!isReady) {
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
   * and the response read timeout. Note that
   * the total timeout is effectively two times the given timeout.
   * @return <code>true</code> if the given HTTP URL has returned
   * response code 200-399 on a HEAD request within the
   * given timeout, otherwise <code>false</code>.
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

  private var _createdJars: List[String] = List()

  /**
   * Adds all JARs in ClassPath to SparkContext.
   */
  private def addClassPathToSparkContext(): Unit = {
    logInfo("Adding ClassPath to SparkContext")
    val cl = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    cl.getURLs.zipWithIndex.foreach({
      /* Plain directories in classpath are bundled into a jar */
      case (url, i) if url.getFile.endsWith("/") =>
          val manifest = new util.jar.Manifest()
          manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
          val target = new JarOutputStream(new FileOutputStream(s"temp_internal$i.jar"), manifest)
          addFileToJar(url.getFile, new File(url.getFile), target)
          target.close()
          _sc.addJar(s"temp_internal$i.jar")
          _createdJars = s"temp_internal$i.jar" :: _createdJars
      /* Jars with a prefix in PROVIDED_JARS are excluded */
      case (url, _)
        if PROVIDED_JARS.exists(prefix => new File(url.getFile).getName.startsWith(prefix)) =>
        logDebug(s"Skipping $url (provided JAR)")
      /* Other jars are added as they are */
      case (url, _) =>
          _sc.addJar(url.getFile)
    })
  }
  private val PROVIDED_JARS = Seq(
    "spark-core-",
    "spark-network-",
    "scala-library-",
    "hadoop-"
  )

  /**
   * Add files to a jar output stream
   *
   * it is used for adding a folder into a jar file
   * This is a recursive method
   *
   * @param root folder root
   * @param source file to be added
   * @param target output jar file stream
   * @return
   */
  private def addFileToJar(root:String, source:File, target:JarOutputStream ): Any = {
    if (source.isDirectory) {
      var name = source.getPath.replace("\\", "/")
      if (name.nonEmpty) {
        if (!name.endsWith("/")) {
          name += "/"
        }
        val entry = new JarEntry(name.replace(root, ""))
        entry.setTime(source.lastModified())
        target.putNextEntry(entry)
        target.closeEntry()
      }
      for (nestedFile <- source.listFiles())
        addFileToJar(root, nestedFile, target)
    } else {
      val entry = new JarEntry(source.getPath.replace("\\", "/").replace(root, ""))
      entry.setTime(source.lastModified())
      target.putNextEntry(entry)
      var in:FileInputStream = null
      try {
        in = new FileInputStream(source)
        IOUtils.copy(in, target)
      } finally {
        if (in != null) {
          in.close()
        }
      }
      target.closeEntry()
    }
  }

  /**
   * Builds a Docker client.
   */
  private def buildDockerClient(): DockerClient = {
    DefaultDockerClient
      .fromEnv()
      .build()
  }

  // Time measurement function
  private def time[A](id: String)(f: => A) = {
    val s = System.nanoTime
    val ret = f
    val result = (System.nanoTime - s) / 1e6
    timingResults = timingResults :+ s"time in $id: $result ms. Result: $ret"
    ret
  }

  protected def fail(i: Int): Unit = {
    if (docker == null) {
      docker = buildDockerClient()
    }
    logDebug(s"Stopping the Spark Worker Container ${DOCKER_SPARK_VELOCITY_CONTAINER}_$i")
    docker.stopContainer(s"${DOCKER_SPARK_VELOCITY_CONTAINER}_$i", 0)
  }

  protected def revive(i: Int): Unit = {
    if (docker == null) {
      docker = buildDockerClient()
    }
    logDebug(s"Re-Starting the Spark Worker Container ${DOCKER_SPARK_VELOCITY_CONTAINER}_$i")
    docker.startContainer(s"${DOCKER_SPARK_VELOCITY_CONTAINER}_$i")
  }
  
  /**
   * Namenode starts in safe (read-only) mode and stays in this state
   * until it reaches a stable state
   * This method checks if namenode is in safe mode
   * and waits until namenode is out of safe mode
   * @return true if namenode is out of safe mode
   */
  protected def checkIfHdfsIsWritable(): Boolean = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", s"hdfs://$hdfsNameNode")
    val nameNodeUri: URI = new URI(s"hdfs://$hdfsNameNode")
    val dfsc:DFSClient = new DFSClient(nameNodeUri, conf)

    val maximumNumberOfIterations = 30
    var numberOfTrialsLeft = maximumNumberOfIterations
    val defaultTimeout = 1000
    var isSafeMode = true
    do {
      isSafeMode = dfsc.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET)
      if (isSafeMode) {
        logDebug(s"Hdsf Namenode is still in safe mode.")
        Thread sleep defaultTimeout
      }
      numberOfTrialsLeft = numberOfTrialsLeft-1
    } while (numberOfTrialsLeft > 0 && isSafeMode)

    !isSafeMode
  }
}

