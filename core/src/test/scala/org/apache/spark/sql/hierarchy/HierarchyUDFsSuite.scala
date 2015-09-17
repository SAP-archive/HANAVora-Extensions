package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, _}
import org.scalatest.FunSuite

import scala.util.Random

// scalastyle:off file.size.limit
class HierarchyUDFsSuite
  extends FunSuite
  with GlobalSapSQLContext
  with HierarchyTestUtils
  with Logging {

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X,Y)] =
      for { x <- xs; y <- ys } yield (x, y)
  }

  def testBinaryUdf(udf: String, expected: Set[Row], buildStrategy: String): Unit = {
    val rdd = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    sc.conf.set("hierarchy.always", buildStrategy)
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT name, node FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H"""

    val hierarchy = sqlContext.sql(queryString)
    hierarchy.registerTempTable("h")
    val query = s"SELECT l.name, r.name, ${udf}(l.node, r.node) FROM h l, h r"
    val result = sqlContext.sql(query).collect().toSet
    assertResult(expected)(result)
  }

  /* TODO(Weidner): test_join argument will be removed asap prerank is set correctly with join buil
  der */
  def testBinaryUdfWithBuilders(udf: String, expected: Set[Row],
                                test_join: Boolean = true): Unit = {
    test(s"test ${udf} using broadcast builder") {
      testBinaryUdf(udf, expected, "broadcast")
    }
    if(test_join) {
      test(s"test ${udf} using join builder") {
        testBinaryUdf(udf, expected, "join")
      }
    }
  }

  def testUnaryUdf(udf: String, expected: Set[Row], buildStrategy: String): Unit = {
    val rdd = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    sc.conf.set("hierarchy.always", buildStrategy)
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT name, node FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H"""

    val hierarchy = sqlContext.sql(queryString)
    hierarchy.registerTempTable("h10")
    val query = s"SELECT name, ${udf}(node) FROM h10"
    val result = sqlContext.sql(query).collect().toSet
    assertResult(expected)(result)
  }

  /* TODO(Weidner): test_join argument will be removed asap prerank is set correctly with join buil
  der */
  def testUnaryUdfWithBuilders(udf: String, expected: Set[Row],
                               test_join: Boolean = true): Unit = {
    test(s"test ${udf} using broadcast builder") {
      testUnaryUdf(udf, expected, "broadcast")
    }
    if(test_join){
      test(s"test ${udf} using join builder") {
        testUnaryUdf(udf, expected, "join")
      }
    }
  }

  testBinaryUdfWithBuilders("IS_DESCENDANT", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false)))

  testBinaryUdfWithBuilders("IS_DESCENDANT_OR_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", true)))

  testBinaryUdfWithBuilders("IS_ANCESTOR", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false)))

  testBinaryUdfWithBuilders("IS_ANCESTOR_OR_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", true)))

  testBinaryUdfWithBuilders("IS_PARENT", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false)))

  testBinaryUdfWithBuilders("IS_CHILD", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false)))

  testUnaryUdfWithBuilders("PRE_RANK", Set(
    Row("Animal", 1),
    Row("Mammal", 2),
    Row("Oviparous", 3)), false)

  testBinaryUdfWithBuilders("IS_FOLLOWING", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", true),
    Row("Oviparous", "Oviparous", false)), false)

  testBinaryUdfWithBuilders("IS_PRECEDING", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", true),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false)), false)

  testUnaryUdfWithBuilders("IS_ROOT", Set(
    Row("Animal", true),
    Row("Mammal", false),
    Row("Oviparous", false)))

  testUnaryUdfWithBuilders("LEVEL", Set(
    Row("Animal", 1),
    Row("Mammal", 2),
    Row("Oviparous", 2)))
}
