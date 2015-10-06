package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.{GlobalSapSQLContext, Row, GlobalSapSQLContext$}
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random

/**
 * Test suite for using a hierarchy in a view.
 */
// scalastyle:off file.size.limit
// scalastyle:off magic.number
class HierarchyViewsSuite
  extends FunSuite
  with BeforeAndAfter
  with GlobalSapSQLContext
  with HierarchyTestUtils
  with Logging {

  before {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.info(s"tSrc: ${tRdd.collect().mkString("|")}")
    tSrc.registerTempTable("t_src")
  }

  test("I can create a hierarchy view") {
  sqlContext.sql(s""" CREATE VIEW HV AS SELECT * FROM HIERARCHY (
                    | USING h_src AS v
                    | JOIN PARENT u ON v.pred = u.succ
                    | START WHERE pred IS NULL
                    | SET Node
                    | ) AS H""".stripMargin)

    val result = sqlContext.sql(
      s"""
      | SELECT A.name, B.address, LEVEL(A.Node)
      | FROM HV A FULL OUTER JOIN t_src B
      | ON A.name = B.name
      """.stripMargin).collect()

    val expected = Set(
      Row("THE BOSS", "Nice Street", 1),
      Row("The Other Middle Manager", null, 2),
      Row("The Middle Manager", "Acceptable Street", 2),
      Row("Senior Developer", "Near-Acceptable Street", 3),
      Row("Minion 1", null, 3),
      Row("Minion 2", null, 4),
      Row("Minion 3", "The Street", 4),
      Row(null, "Death Star", null)
    )
    assertResult(expected)(result.toSet)
  }

  test("I can self-join a hierarchy view") {
    sqlContext.sql(
      s"""
          | CREATE VIEW HV AS SELECT * FROM HIERARCHY (
          | USING h_src AS v
          | JOIN PARENT u ON v.pred = u.succ
          | START WHERE pred IS NULL
          | SET Node
          | ) AS H""".stripMargin)

    val result = sqlContext.sql(
      s"""
         | SELECT A.name, B.name
         | FROM HV A, HV B
         | WHERE IS_CHILD(A.Node, B.Node)=true
       """.stripMargin).collect()

    val expected = Set(
      Row("Senior Developer" , "The Middle Manager"),
      Row("Minion 3" , "Senior Developer"),
      Row("Minion 2" , "Senior Developer"),
      Row("The Other Middle Manager" , "THE BOSS"),
      Row("The Middle Manager" , "THE BOSS"),
      Row("Minion 1" , "The Middle Manager")
    )

    assertResult(expected)(result.toSet)
  }

  test("I can reuse hierarchy view") {
    sqlContext.sql(
      s"""
         | CREATE VIEW HV1 AS SELECT * FROM HIERARCHY (
         | USING h_src AS v
         | JOIN PARENT u ON v.pred = u.succ
         | START WHERE pred IS NULL
         | SET Node
         | ) AS H""".stripMargin)

    sqlContext.sql(
      s"""
         | CREATE VIEW HV2 AS SELECT A.name AS childName, B.name AS parentName
         | FROM HV1 A, HV1 B
         | WHERE IS_CHILD(A.Node, B.Node)=true
       """.stripMargin)

    val result = sqlContext.sql(
    s"""
       | SELECT A.childName, B.address
       | FROM HV2 A FULL OUTER JOIN t_src B
       | ON A.childName = B.name
       | WHERE A.parentName = 'THE BOSS'
     """.stripMargin
    ).collect()

    val expected = Set(
      Row("The Middle Manager", "Acceptable Street"),
      Row("The Other Middle Manager", null)
    )

    assertResult(expected)(result.toSet)
  }

  // (YH) This might be the funniest test I have ever written! :-)
  test("I can join different hierarchies together") {
    val rdd = sc.parallelize(animalsHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("animals_src")

    sqlContext.sql(
      s"""
         | CREATE VIEW AnimalsView AS SELECT * FROM HIERARCHY (
         | USING animals_src AS v
         | JOIN PARENT u ON v.pred = u.succ
         | START WHERE pred IS NULL
         | SET Node
         | ) AS H""".stripMargin)

    sqlContext.sql(
      s"""
         | CREATE VIEW OrgView AS SELECT * FROM HIERARCHY (
         | USING h_src AS v
         | JOIN PARENT u ON v.pred = u.succ
         | START WHERE pred IS NULL
         | SET Node
         | ) AS H""".stripMargin)

    val result = sqlContext.sql(
    s"""
       | SELECT A.name, B.name
       | FROM AnimalsView A FULL OUTER JOIN OrgView B
       | ON IS_CHILD(A.Node, B.Node)
     """.stripMargin
    ).collect()

    val expected = Set(
      Row(null,"Minion 3"),
      Row("Carnivores","The Middle Manager"),
      Row("Herbivores","The Middle Manager"),
      Row(null,"Minion 1"),
      Row(null,"Senior Developer"),
      Row(null,"The Other Middle Manager"),
      Row("Mammal","THE BOSS"),
      Row("Oviparous","THE BOSS"),
      Row("Animal",null),
      Row(null,"Minion 2"))
    assertResult(expected)(result.toSet)
  }
}
