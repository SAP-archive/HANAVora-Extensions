package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.analysis.OverrideFunctionRegistry


private[sql] class HiveFunctionRegistryProxy
  extends HiveFunctionRegistry
  with OverrideFunctionRegistry{
  override def caseSensitive: Boolean = false
}
