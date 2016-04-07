package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, _}
import org.scalatest.FunSuite

import scala.util.Random

// scalastyle:off file.size.limit
// scalastyle:off magic.number
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
    createAnimalsTable(sqlContext)
    sc.conf.set("hierarchy.always", buildStrategy)
    val hierarchy = sqlContext.sql(hierarchySQL(animalsTable, "name, node"))
    hierarchy.registerTempTable("h")
    val query = s"SELECT l.name, r.name, $udf(l.node, r.node) FROM h l, h r"
    val result = sqlContext.sql(query).collect().toSet
    assertResult(expected)(result)
  }

  def testBinaryUdfWithBuilders(udf: String, expected: Set[Row]): Unit = {
    test(s"test $udf using broadcast builder") {
      testBinaryUdf(udf, expected, "broadcast")
    }
    test(s"test $udf using join builder") {
      testBinaryUdf(udf, expected, "join")
    }
  }

  def testUnaryUdf(udf: String, expected: Set[Row], buildStrategy: String): Unit = {
    createAnimalsTable(sqlContext)
    sc.conf.set("hierarchy.always", buildStrategy)
    val hierarchy = sqlContext.sql(hierarchySQL(animalsTable, "name, node"))
    hierarchy.registerTempTable("h10")
    val query = s"SELECT name, $udf(node) FROM h10"
    val result = sqlContext.sql(query).collect().toSet
    assertResult(expected)(result)
  }

  /* TODO(Weidner): test_join argument will be removed asap prerank is set correctly with join buil
  der */
  def testUnaryUdfWithBuilders(udf: String, expected: Set[Row],
                               testJoin: Boolean = true): Unit = {
    test(s"test $udf using broadcast builder") {
      testUnaryUdf(udf, expected, "broadcast")
    }
    if(testJoin){
      test(s"test $udf using join builder") {
        testUnaryUdf(udf, expected, "join")
      }
    }
  }

  testBinaryUdfWithBuilders("IS_DESCENDANT", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", true),
    Row("Carnivores", "Mammal", true),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", true),
    Row("Herbivores", "Mammal", true),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_DESCENDANT_OR_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", true),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", true),
    Row("Carnivores", "Mammal", true),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", true),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", true),
    Row("Herbivores", "Mammal", true),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", true)))

  testBinaryUdfWithBuilders("IS_ANCESTOR", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Animal", "Carnivores", true),
    Row("Animal", "Herbivores", true),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", true),
    Row("Mammal", "Herbivores", true),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_ANCESTOR_OR_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Animal", "Carnivores", true),
    Row("Animal", "Herbivores", true),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", true),
    Row("Mammal", "Herbivores", true),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", true),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", true),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", true)))

  testBinaryUdfWithBuilders("IS_PARENT", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", true),
    Row("Animal", "Oviparous", true),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", true),
    Row("Mammal", "Herbivores", true),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_CHILD", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", true),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", true),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", true),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", true),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_SIBLING", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", true),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", true),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", true),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", true),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", true),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", true),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", true)))

  testBinaryUdfWithBuilders("IS_SIBLING_OR_SELF", Set(
    Row("Animal", "Animal", true),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", true),
    Row("Mammal", "Oviparous", true),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", true),
    Row("Oviparous", "Oviparous", true),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", true),
    Row("Carnivores", "Herbivores", true),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", true),
    Row("Herbivores", "Herbivores", true)))

  testUnaryUdfWithBuilders("PRE_RANK", Set(
    Row("Animal", 1),
    Row("Mammal", 2),
    Row("Carnivores", 3),
    Row("Herbivores", 4),
    Row("Oviparous", 5)), testJoin = false)

  testUnaryUdfWithBuilders("POST_RANK", Set(
    Row("Animal", 5),
    Row("Mammal", 3),
    Row("Carnivores", 1),
    Row("Herbivores", 2),
    Row("Oviparous", 4)), testJoin = false)

  testBinaryUdfWithBuilders("IS_FOLLOWING", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", false),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", true),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", true),
    Row("Oviparous", "Herbivores", true),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", false),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", false),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", false),
    Row("Herbivores", "Carnivores", true),
    Row("Herbivores", "Herbivores", false)))

  testBinaryUdfWithBuilders("IS_PRECEDING", Set(
    Row("Animal", "Animal", false),
    Row("Animal", "Mammal", false),
    Row("Animal", "Oviparous", false),
    Row("Animal", "Carnivores", false),
    Row("Animal", "Herbivores", false),
    Row("Mammal", "Animal", false),
    Row("Mammal", "Mammal", false),
    Row("Mammal", "Oviparous", true),
    Row("Mammal", "Carnivores", false),
    Row("Mammal", "Herbivores", false),
    Row("Oviparous", "Animal", false),
    Row("Oviparous", "Mammal", false),
    Row("Oviparous", "Oviparous", false),
    Row("Oviparous", "Carnivores", false),
    Row("Oviparous", "Herbivores", false),
    Row("Carnivores", "Animal", false),
    Row("Carnivores", "Mammal", false),
    Row("Carnivores", "Oviparous", true),
    Row("Carnivores", "Carnivores", false),
    Row("Carnivores", "Herbivores", true),
    Row("Herbivores", "Animal", false),
    Row("Herbivores", "Mammal", false),
    Row("Herbivores", "Oviparous", true),
    Row("Herbivores", "Carnivores", false),
    Row("Herbivores", "Herbivores", false)))

  testUnaryUdfWithBuilders("IS_ROOT", Set(
    Row("Animal", true),
    Row("Mammal", false),
    Row("Oviparous", false),
    Row("Carnivores", false),
    Row("Herbivores", false)))

  testUnaryUdfWithBuilders("IS_LEAF", Set(
    Row("Animal", false),
    Row("Mammal", false),
    Row("Oviparous", true),
    Row("Carnivores", true),
    Row("Herbivores", true)))

  testUnaryUdfWithBuilders("LEVEL", Set(
    Row("Animal", 1),
    Row("Mammal", 2),
    Row("Oviparous", 2),
    Row("Carnivores", 3),
    Row("Herbivores", 3)))
}
