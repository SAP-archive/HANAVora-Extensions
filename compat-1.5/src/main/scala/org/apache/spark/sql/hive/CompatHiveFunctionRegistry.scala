package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleFunctionRegistry

object CompatHiveFunctionRegistry {
  def apply(catalystConf: CatalystConf): HiveFunctionRegistry =
    /* Spark >=1.5 && <= 1.5.2 */
    new HiveFunctionRegistry(new SimpleFunctionRegistry)
    /* TODO: Spark >=1.5.3: new HiveFunctionRegistry(new SimpleFunctionRegistry, hiveExecution) */
}
