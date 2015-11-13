package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.OverrideFunctionRegistry

object CompatHiveFunctionRegistry {
  def apply(catalystConf: CatalystConf): HiveFunctionRegistry with OverrideFunctionRegistry = {
    new HiveFunctionRegistry with OverrideFunctionRegistry {
      override def conf: CatalystConf = catalystConf
    }
  }
}
