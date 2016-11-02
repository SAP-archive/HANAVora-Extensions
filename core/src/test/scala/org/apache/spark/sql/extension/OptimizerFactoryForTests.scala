package org.apache.spark.sql.extension


object OptimizerFactoryForTests {
  def default() = {
    if (org.apache.spark.SPARK_VERSION.contains("1.6.2")) {
      ExtendableOptimizer162.defaultOptimizer
    } else {
      ExtendableOptimizer161.defaultOptimizer
    }
  }
}
