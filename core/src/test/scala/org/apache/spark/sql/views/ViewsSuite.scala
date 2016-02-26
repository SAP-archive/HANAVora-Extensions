package org.apache.spark.sql.views

import org.apache.spark.Logging
import org.apache.spark.sql.hierarchy.HierarchyTestUtils
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
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
}
