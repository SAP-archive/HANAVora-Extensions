package org.apache.spark.sql.views

import org.apache.spark.Logging
import org.apache.spark.sql.hierarchy.HierarchyTestUtils
import org.apache.spark.sql.{SQLConf, GlobalSapSQLContext, Row}
import org.scalatest.FunSuite

import scala.util.Random

class ViewsSuite extends FunSuite
  with HierarchyTestUtils
  with GlobalSapSQLContext
  with Logging {

  test("Rewire nested view after dropping and recreating nested view (bug 104634)") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val rdd2 = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val animals = sqlContext.createDataFrame(rdd2).cache()
    animals.registerTempTable("animals")

    sqlContext.sql("CREATE VIEW v1 AS SELECT * FROM hSrc")
    sqlContext.sql("CREATE VIEW v2 AS SELECT * FROM v1 WHERE ord = 1")

    // drop the view v1.
    sqlContext.catalog.unregisterTable("v1" :: Nil)

    // create v1 again using different schema.
    sqlContext.sql("CREATE VIEW v1 AS SELECT * FROM animals")

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v2").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Rewire nested view after dropping and recreating nested table (bug 104634)") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("t1")

    sqlContext.sql("CREATE VIEW v1 AS SELECT * FROM t1 WHERE ord = 1")

    // drop the table.
    sqlContext.catalog.unregisterTable("t1" :: Nil)

    // create the table again using different schema.
    val rdd2 = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val animals = sqlContext.createDataFrame(rdd2).cache()
    animals.registerTempTable("t1")

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v1").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Rewire nested dimension view after dropping and recreating " +
    "nested dimension view (bug 104634)") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val rdd2 = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val animals = sqlContext.createDataFrame(rdd2).cache()
    animals.registerTempTable("animals")

    sqlContext.sql("CREATE DIMENSION VIEW v1 AS SELECT * FROM hSrc")
    sqlContext.sql("CREATE DIMENSION VIEW v2 AS SELECT * FROM v1 WHERE ord = 1")

    // drop the dimension view v1.
    sqlContext.catalog.unregisterTable("v1" :: Nil)

    // create dimension v1 again using different schema.
    sqlContext.sql("CREATE DIMENSION VIEW v1 AS SELECT * FROM animals")

    // now v2 should work using the updated v1.
    val result = sqlContext.sql("SELECT * FROM v2").collect()
    assertResult(Seq(
      Row("Animal", null, 1, 1),
      Row("Mammal", 1, 2, 1)).toSet)(result.toSet)
  }

  test("Views names case-sensitivity is handled correctly") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val originalConf = sqlContext.conf.caseSensitiveAnalysis

    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql("CREATE VIEW VIEW1 AS SELECT * FROM hSrc")
      intercept[RuntimeException] {
        sqlContext.sql("CREATE VIEW view1 AS SELECT * FROM hSrc")
      }

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")
      sqlContext.sql("CREATE VIEW VIEW2 AS SELECT * FROM hSrc")
      // this will not throw (although we use the same identifier name).
      sqlContext.sql("CREATE VIEW view2 AS SELECT * FROM hSrc")
    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }

  test("CREATE VIEW IF NOT EXISTS handles case-sensitivity correctly") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val originalConf = sqlContext.conf.caseSensitiveAnalysis

    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql("CREATE VIEW view1 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE VIEW IF NOT EXISTS VIEW1 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")

      sqlContext.sql("CREATE VIEW view2 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE VIEW IF NOT EXISTS VIEW2 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")

    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }

  test("CREATE DIMENSION VIEW IF NOT EXISTS handles case-sensitivity correctly") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val originalConf = sqlContext.conf.caseSensitiveAnalysis

    try {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "false")
      sqlContext.sql("CREATE DIMENSION VIEW view1 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE DIMENSION VIEW IF NOT EXISTS VIEW1 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")

      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, "true")

      sqlContext.sql("CREATE DIMENSION VIEW view2 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")
      // should not throw.
      sqlContext.sql("CREATE DIMENSION VIEW IF NOT EXISTS VIEW2 AS SELECT * " +
        "FROM hSrc USING com.sap.spark.dstest")

    } finally {
      sqlContext.setConf(SQLConf.CASE_SENSITIVE.key, originalConf.toString)
    }
  }
}
