package com.sap.spark.docker

import java.io.{IOException, FileInputStream, File, FileOutputStream}
import java.net.{URL, URLClassLoader}
import java.util.jar.{JarEntry, JarOutputStream, Attributes}

import com.sap.spark.WithSparkContext
import org.apache.commons.io.IOUtils
import org.apache.spark.Logging

/**
 * Adds the classpath to a [[org.apache.spark.SparkContext]].
 */
private[docker] trait AddJars extends Logging {
  self: WithSparkContext =>

  private var _createdJars: List[String] = List()

  /**
   * Adds all JARs in ClassPath to SparkContext.
   */
  private[docker] def addClassPathToSparkContext(): Unit = {
    logInfo("Adding ClassPath to SparkContext")
    val cl = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    cl.getURLs.zipWithIndex.foreach({
      /* Plain directories in classpath are bundled into a jar */
      case (url, i) if url.getFile.endsWith("/") =>
        val manifest = new java.util.jar.Manifest()
        manifest.getMainAttributes.put(Attributes.Name.MANIFEST_VERSION, "1.0")
        val target = new JarOutputStream(new FileOutputStream(s"temp_internal$i.jar"), manifest)
        addFileToJar(url.getFile, new File(url.getFile), target)
        target.close()
        sc.addJar(s"temp_internal$i.jar")
        _createdJars = s"temp_internal$i.jar" :: _createdJars
      /* Jars with a prefix in PROVIDED_JARS are excluded */
      case (url, _) if isProvidedJar(url) =>
        logDebug(s"Skipping $url (provided JAR)")
      /* Other jars are added as they are */
      case (url, _) =>
        sc.addJar(url.getFile)
    })
  }

  private[docker] def cleanUpCreatedJars(): Unit =
    _createdJars.foreach(path => {
      try {
        new File(path).delete()
      } catch {
        case ioe: IOException => logError(ioe.getMessage)
      }
    })

  private def isProvidedJar(url: URL): Boolean = {
    val file = new File(url.getFile)
    PROVIDED_JARS.exists(prefix => file.getName.startsWith(prefix)) ||
      file.getAbsolutePath.contains("/jre/lib/")
  }

  private val PROVIDED_JARS = Seq(
    "spark-core-",
    "spark-network-",
    "spark-launcher-",
    "spark-unsafe-",
    "chill_", "child-java-", "kryo-", "jetty-", "netty-", "akka-", "tachyon-", "parquet-",
    "scala-library-", "scala-compiler-",
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
  private def addFileToJar(root: String, source: File, target: JarOutputStream): Any = {
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
      var in: FileInputStream = null
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

}
