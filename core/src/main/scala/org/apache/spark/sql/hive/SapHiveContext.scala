package org.apache.spark.sql.hive

import org.apache.spark.SparkContext
import org.apache.spark.sql.{CommonSapSQLContext, SQLContext}
import org.apache.spark.sql.execution.CacheManager
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.hive.client.{ClientInterface, ClientWrapper}

/**
 * This context provides extended [[HiveContext]] functionality such as hierarchies, enhanced data
 * sources API with support for aggregates pushdown, etc.
 *
 * @see [[CommonSapSQLContext]]
 */
class SapHiveContext(
    @transient sparkContext: SparkContext,
    cacheManager: CacheManager,
    listener: SQLListener,
    @transient execHive: ClientWrapper,
    @transient metaHive: ClientInterface,
    isRootContext: Boolean)
  extends ExtendableHiveContext(
    sparkContext,
    cacheManager,
    listener,
    execHive,
    metaHive,
    isRootContext)
  with CommonSapSQLContext {

  def this(sc: SparkContext) =
    this(sc, new CacheManager, SQLContext.createListenerAndUI(sc), null, null, true)

  override def newSession(): HiveContext =
    new SapHiveContext(
      sparkContext = this.sparkContext,
      cacheManager = this.cacheManager,
      listener = this.listener,
      executionHive.newSession(),
      metadataHive.newSession(),
      isRootContext = false)
}
