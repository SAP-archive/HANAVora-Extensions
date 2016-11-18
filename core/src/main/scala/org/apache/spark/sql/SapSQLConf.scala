package org.apache.spark.sql

import org.apache.spark.sql.SQLConf.SQLConfEntry._

/** SQL context properties */
object SapSQLConf {

  val HIVE_EMULATION =
    booleanConf(
      key = "spark.vora.hive_emulation",
      defaultValue = Some(false),
      doc =
        """Whether to emulate hive-like behavior (enable support for database
          |prefixed identifiers, support USE and other statements) or not.
          |The usage of this flag is experimental.""".stripMargin)
}
