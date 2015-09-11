package org.apache.spark.sql

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Allows to create a new SapSQLContext with a given
 * configuration. Since only a single SapSQLContext
 * is allowed to exist at a time this factory handles
 * the proper shutdown of an existing context before
 * starting a new one.
 *
 */
object SapSQLContextEnv {

  val DEFAULTS = Map(
    "spark.driver.allowMultipleContexts" -> "true",
    "spark.sql.shuffle.partitions"       -> "4",
    /* XXX: Disable join broadcast */
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.broadcast.factory" -> "org.apache.spark.broadcast.HttpBroadcastFactory",
    "spark.shuffle.spill"     -> "false",
    "spark.shuffle.compress"  -> "false",
    "spark.ui.enabled"        -> "false",
    "spark.ui.showConsoleProgress" -> "false",
    /*
     * TODO: Use old Parquet API, new one has bug:
     *       https://issues.apache.org/jira/browse/SPARK-6330
     *       We should be able to remove this with Spark 1.4.0.
     */
    "spark.sql.parquet.useDataSourceApi" -> "false"
  )

  // singletons
  var sparkConf: SparkConf = null
  var sparkContext: SparkContext = null
  var sapSQLContext: SapSQLContext = null

  /**
   * Get a SapSQLContext with the given sparkConf properties.
   * If a context already exists the existing context will
   * first be closed before the new context gets started.
   *
   * @param sparkConfProperties Map of spark properties
   *                            handed to the context
   * @param master The master string (default: local[2])
   * @param loadDefaults Load some default properties.
   * @return
   */
  def getContext(sparkConfProperties: Map[String,String] = Map(),
                 master: String = "local[2]",
                 loadDefaults: Boolean = true): SapSQLContext = {
    this.synchronized {
      if (sparkConf != null) {
        SapSQLContextEnv.shutdownContext()
      }

      sparkConf = new SparkConf(loadDefaults = false)
      if(loadDefaults) {
        DEFAULTS.foreach {
          case (key: String, value: String) => sparkConf.set(key, value)
        }
      }
      sparkConfProperties.foreach {
        case (key: String, value: String) => sparkConf.set(key, value)
      }
      sparkContext = new SparkContext(master, "test", sparkConf)
      sapSQLContext = new SapSQLContext(sparkContext)

      sapSQLContext
    }
  }

  /**
   * Shutdown an existing SapSQLContext and its SparkContext.
   * Gets implicitly called by #getContext(). Gracefully
   * ignored if no context exists.
   */
  def shutdownContext(): Unit = {
    if(sparkConf != null) {
      this.synchronized {
        sapSQLContext.catalog.unregisterAllTables()
        sapSQLContext = null

        // sparkContext.cancelAllJobs()
        sparkContext.stop()
        sparkContext = null

        sparkConf = null
      }
    }
  }
}
