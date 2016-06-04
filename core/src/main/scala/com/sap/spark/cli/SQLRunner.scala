package com.sap.spark.cli

import java.io._

import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{Logging, SparkContext}

import scala.annotation.tailrec

protected[cli] case class CLIOptions(
    sqlFiles: List[String] = Nil, output: Option[String] = None)

/**
  * Command line interface to execute SQL statements via [[SQLContext.sql]].
  */
object SQLRunner extends Logging {

  /**
    * Iterate the stream line by line. Newline characters are removed from the result.
    *
    * @param stream stream to iterate
    */
  private implicit class FatInputStream(stream: InputStream) {
    def toIterator: Iterator[String] = {
      val reader = new BufferedReader(new InputStreamReader(stream))
      Iterator
        .continually(Option(reader.readLine()))
        .takeWhile(_.isDefined)
        .map(_.get)
    }
  }

  /**
    * Write a single row to an [[OutputStream]].
    */
  private def writeRow(row: Row, outputStream: OutputStream): Unit = {
    val writer = new OutputStreamWriter(outputStream)
    writer.write(row.mkString(",") + "\n")
    writer.close()
  }

  /**
    * Runs all commands in the given stream. Commands are separated by ';', and empty commands or
    * commands consisting of whitespace only are omitted.
    *
    * @return Sequence of results
    */
  def sql(inputStream: InputStream, outputStream: OutputStream): Unit = {
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())

    def executeWithLogging(cmd: String): DataFrame = {
      logInfo(s"executing SQL: $cmd")
      sqlContext.sql(cmd)
    }

    scala.io.Source.fromInputStream(inputStream)
      .getLines()
      .mkString("\n") // newlines are newlines, again
      .split(';')
      .map(_.trim)
      .filter(_.nonEmpty) // throw away no-op commands
      .map(executeWithLogging)
      .foreach(_.collect().foreach(writeRow(_, outputStream)))
  }

  //                          //
  // CLI implementation below //
  //                          //

  private val USAGE = "Usage: <cmd> input.sql [input2.sql ...] [-o output.csv]"

  @tailrec
  def parseOpts(remainingArguments: List[String],
                options: CLIOptions = CLIOptions()): CLIOptions = {
    remainingArguments match {
      case "-o" :: output_file :: tail =>
        parseOpts(tail, CLIOptions(options.sqlFiles, Some(output_file)))
      case input_file :: tail =>
        parseOpts(
            tail, CLIOptions(options.sqlFiles :+ input_file, options.output))
      case Nil => options // end of recursion
    }
  }

  /**
    * Entry point for SQL tests with user defined input.
    *
    * @param args command line arguments
    */
  def main(args: Array[String]): Unit = {
    def fail(msg: String = USAGE): Unit = {
      logError(msg)
      System.exit(1)
    }

    val opts = parseOpts(args.toList)

    val outputStream: OutputStream = opts.output match {
      case Some(filename) => new FileOutputStream(new File(filename))
      case None => System.out
    }

    opts.sqlFiles
      .map((string: String) => new FileInputStream(new File(string)))
      .foreach(sql(_, outputStream))
  }
}
