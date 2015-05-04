package corp.sap.spark

import java.io._
import java.net.{URLClassLoader, URLConnection, HttpURLConnection, URL}
import java.util
import java.util.jar.{JarEntry, JarOutputStream, Attributes}

import com.spotify.docker.client.messages.{HostConfig, Container, ContainerCreation, ContainerConfig}
import com.spotify.docker.client.{DockerException, ContainerNotFoundException, DefaultDockerClient, DockerClient}
import org.apache.commons.io.IOUtils
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Shares a `SparkContext` connected to Docker initiated Cluster
 *  between all tests in a suite and closes it at the end
 */
trait DockerSparkContext extends BeforeAndAfterAll  with Logging { self: Suite =>
  @transient private var _sc: SparkContext = _
  def sc: SparkContext = _sc
  private var docker: DockerClient = _

  @transient private var _hosts: Seq[String] = Seq()
  def hosts: Seq[String] = _hosts

  @transient private var _zookeeperIp: String = _
  def zookeeperIp: String = _zookeeperIp

  @transient private var _sparkMasterIp: String = _
  def sparkMasterIp: String = _sparkMasterIp

  val ONEGIGABYTE:Long = 1*1024*1024*1024
  val userHome = System.getProperty("user.home")

  var createdJars: List[String] = List()
  private var resetDockerSparkCluster:Boolean = _

  def sparkConf: SparkConf = {
    val conf = new SparkConf(false)
    /* XXX: Prevent 200 partitions on shuffle */
    conf.set("spark.sql.shuffle.partitions", "4")
    /* XXX: Disable join broadcast */
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
    conf.set("spark.shuffle.spill", "false")
    conf.set("spark.shuffle.compress", "false")
    conf.set("spark.ui.enabled", "false")
  }

  override def beforeAll() {
    _sc = new SparkContext(s"spark://$sparkMasterIp:7077", "docker-spark-test" , sparkConf)
    val cl = ClassLoader.getSystemClassLoader
    var i = 0
    cl.asInstanceOf[URLClassLoader].getURLs.foreach(url => {
      if (url.getFile.endsWith("/")) {
        val manifest:util.jar.Manifest = new util.jar.Manifest()
        manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
        val target:JarOutputStream
          = new JarOutputStream(new FileOutputStream(s"temp_internal$i.jar"), manifest)
        addFileToJar(url.getFile, new File(url.getFile), target)
        target.close()
        _sc.addJar(s"temp_internal$i.jar")
        createdJars = s"temp_internal$i.jar" :: createdJars
        i += 1
      } else {
        _sc.addJar(url.getFile)
      }
    })
    super.beforeAll()
  }
  override def afterAll() {
    _sc.stop()
    _sc = null
    stopDockerSparkCluster()
    createdJars.foreach( path => {
      try {
        new File(path).delete()
      } catch {
        case ioe: IOException => logError(ioe.getMessage)
      }
    })
    super.afterAll()
  }

  /**
   * Starts a docker containers to build a spark cluster.
   *
   * @param dataPath folder that will be shared with spark workers
   * the folder will be visible to velocity server
   * @param numberOfWorkers number of workers in spark cluster
   * @param resetDockerSparkCluster should be set to true to use
   * a fresh copy of cluster
   * @param ramLimit limits the ram usage of docker containers
   * ramLimit uses process groups to limit ram and cpu
   * @return sparkMasterIp Ip address of spark master node
   * it can be used to start a sparkContext
   */
  def startDockerSparkCluster(dataPath:String = userHome,
                             numberOfWorkers:Int = 2,
                             resetDockerSparkCluster:Boolean = true,
                             ramLimit:Long = ONEGIGABYTE
                               ): String = {
    this.resetDockerSparkCluster = resetDockerSparkCluster
    docker = new DefaultDockerClient("unix:///var/run/docker.sock")
    if (resetDockerSparkCluster) {
      stopRemoveAllContainers()
    }
    _sparkMasterIp = try {
      (docker inspectContainer "spark_master").networkSettings().ipAddress()
    } catch {
      case cne:ContainerNotFoundException =>
      stopRemoveAllContainers()
      _zookeeperIp = startZookeeper(ramLimit)
      val masterConfig: ContainerConfig = ContainerConfig.builder()
        .image("sparkvelocity").cmd("/start-master.sh")
        .tty(true).memory(ramLimit).build()
      val masterHostConfig: HostConfig = HostConfig.builder()
        .publishAllPorts(true).build()

      val masterCreation: ContainerCreation = docker.createContainer(masterConfig, "spark_master")
      val masterId: String = masterCreation.id()
      docker.startContainer(masterId, masterHostConfig)

      val newSparkMasterIp = docker.inspectContainer(masterId).networkSettings().ipAddress()
      waitUntilNodeIsReady(newSparkMasterIp, "8080")

      val hostConfig: HostConfig = HostConfig.builder()
        .publishAllPorts(true).links(s"spark_master:spark_master")
        .binds(s"$dataPath:$dataPath:ro")
        .build()

      val workerConfig: ContainerConfig = ContainerConfig.builder()
        .image("sparkvelocity").volumes(dataPath)
        .cmd("/start-worker.sh").tty(true)
        .memory(ramLimit)
        .build()

      for (i <- 1 to numberOfWorkers) {
        val creation: ContainerCreation = docker.createContainer(workerConfig,s"spark_worker$i")
        val id = creation.id()
        docker.startContainer(id, hostConfig)
      }
      newSparkMasterIp
    }
    checkHostsAndPaths(numberOfWorkers)
    _sparkMasterIp
  }

  /**
  * TODO: This method will be reduced in future
  */
  def checkHostsAndPaths(numberOfWorkers:Int): Unit = {
    _zookeeperIp = docker.inspectContainer(s"spark_zookeeper").networkSettings().ipAddress()
    for (i <- 1 to numberOfWorkers) {
      var ip = docker.inspectContainer(s"spark_worker$i").networkSettings().ipAddress()
      waitUntilNodeIsReady(ip, "8081")
      ip += ":2202"
      _hosts:+=ip
    }
  }

  def startZookeeper(ramLimit:Long): String = {
    val zookeeperConfig: ContainerConfig = ContainerConfig.builder()
      .image("jplock/zookeeper")
      .tty(true).memory(ramLimit).build()
    val zookeeperHostConfig: HostConfig = HostConfig.builder()
      .publishAllPorts(true).build()

    val zookeeperCreation: ContainerCreation
      = docker.createContainer(zookeeperConfig, "spark_zookeeper")
    val zookeeperId: String = zookeeperCreation.id()
    docker.startContainer(zookeeperId, zookeeperHostConfig)

    docker.inspectContainer(zookeeperId).networkSettings().ipAddress()
  }

  def stopDockerSparkCluster() {
    if (resetDockerSparkCluster) {
      stopRemoveAllContainers()
    }
    docker.close()
  }

  /**
   * Stops and removes every container in the local machine
   * 
   * a container cannot be rmeoved without stoppping
   * if a container did not stop before removing an exception occurs
   * this method waits and retries until every container is removed
   */
  def stopRemoveAllContainers() {
    if (docker == null) {
      docker = new DefaultDockerClient("unix:///var/run/docker.sock")
    }
    val defaultTimeout = 1000
    var iterations = 0
    val maximumIterations = 10
    var containers: util.List[Container] = null
    do {
      containers = docker.listContainers(DockerClient.ListContainersParam.allContainers(true))
      for (i <- 0 until containers.size()) {
        try {
          docker.stopContainer(containers.get(i).id(), 0)
          docker.removeContainer(containers.get(i).id())
        } catch {
          case e:DockerException => {
            logDebug("Retrying due to :%s".format(e.getMessage))
            Thread sleep defaultTimeout
          }
        }
      }
      iterations+=1
    } while (containers.size() > 0 && iterations < maximumIterations)
  }

  /**
   * Checks an http port to check if a http service is up
   * 
   * it is used to check if a spark master or spark worker is up
   * 
   * @param ip
   * @param port
   */
  def waitUntilNodeIsReady(ip:String, port:String) {
    var isReady = false
    var iterations = 0
    val maximumIterations = 240
    val defaultTimeout = 1000

    do {
      isReady = pingHttp(s"http://$ip:$port", defaultTimeout)
      if(!isReady) {
        Thread sleep defaultTimeout
      }
      iterations+=1
    } while(!isReady && iterations < maximumIterations)

    if(!isReady) {
      throw new Exception(s"Node at $ip may not be initialized")
    }
  }

  /**
   * Pings a HTTP URL.
   * This effectively sends a HEAD request and returns <code>true</code>
   * if the response code is in the 200-399 range.
   * 
   * @param urlInput The HTTP URL to be pinged.
   * @param timeout The timeout in millis for both the connection timeout
   * and the response read timeout. Note that
   * the total timeout is effectively two times the given timeout.
   * @return <code>true</code> if the given HTTP URL has returned
   * response code 200-399 on a HEAD request within the
   * given timeout, otherwise <code>false</code>.
   */
  def pingHttp(urlInput:String, timeout:Int): Boolean = {
    // Otherwise an exception may be thrown on invalid SSL certificates:
    val url = urlInput.replaceFirst("^https", "http")
    try {
      val tempConnection:URLConnection = new URL(url).openConnection()
      val connection:HttpURLConnection = tempConnection.asInstanceOf[HttpURLConnection]
      connection.setConnectTimeout(timeout)
      connection.setReadTimeout(timeout)
      connection.setRequestMethod("HEAD")
      val responseCode:Int = connection.getResponseCode
      200 <= responseCode && responseCode <= 399
    } catch {
      case ioe: IOException => false
    }
  }

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
  def addFileToJar(root:String, source:File, target:JarOutputStream ): Any = {
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
      target.closeEntry();
    }
  }
}

