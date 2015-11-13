package org.apache.spark.sql.hive

package object compat {

  val HIVE_VERSION =
    org.apache.spark.SPARK_VERSION match {
      case x if x startsWith "1.4" =>
        HiveShim.getClass.getMethod("version")
          .invoke(HiveShim).asInstanceOf[String]
      case x if x startsWith "1.5" =>
        HiveContext.getClass.getMethod("hiveExecutionVersion")
          .invoke(HiveContext).asInstanceOf[String]
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }

}
