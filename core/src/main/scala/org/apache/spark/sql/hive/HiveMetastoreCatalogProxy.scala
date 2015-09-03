package org.apache.spark.sql.hive

import org.apache.spark.sql.TemporaryFlagProxyCatalog
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.hive.client.ClientInterface

/*
 * HiveMetastoreCatalogProxy is needed as original HiveMetastoreCatalog
 * is not case sensitive and it does not throw the same type of exceptions as
 * SimpleCatalog throws. Methods are overridden with SimpleCatalog implementation
 */
private[sql] class HiveMetastoreCatalogProxy(client: ClientInterface, hive: HiveContext)
  extends HiveMetastoreCatalog(client: ClientInterface, hive: HiveContext)
with TemporaryFlagProxyCatalog
