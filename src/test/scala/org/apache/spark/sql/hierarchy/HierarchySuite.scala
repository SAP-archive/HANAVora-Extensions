package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType, Node, StringType}
import org.scalatest.FunSuite

import scala.util.Random

case class HierarchyRow(name : String, pred : Option[Long], succ : Long, ord : Int)

case class AddressRow(name : String, address : String)

case class PartialResult(path: Seq[Long], pk: Long)

// scalastyle:off magic.number
// scalastyle:off file.size.limit

class HierarchySuite extends FunSuite
with GlobalVelocitySQLContext with Logging {

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) : Traversable[(X,Y)] =
      for { x <- xs; y <- ys } yield (x, y)
  }

  def adjacencyList : Seq[HierarchyRow] = Seq(
    HierarchyRow("THE BOSS", None, 1L, 1),
    HierarchyRow("The Middle Manager", Some(1L), 2L, 1),
    HierarchyRow("The Other Middle Manager", Some(1L), 3L, 2),
    HierarchyRow("Senior Developer", Some(2L), 4L, 1),
    HierarchyRow("Minion 1", Some(2L), 5L, 2),
    HierarchyRow("Minion 2", Some(4L), 6L, 1),
    HierarchyRow("Minion 3", Some(4L), 7L, 2)
  )

  def addresses : Seq[AddressRow] = Seq(
    AddressRow("THE BOSS", "Nice Street"),
    AddressRow("The Middle Manager", "Acceptable Street"),
    AddressRow("Senior Developer", "Near-Acceptable Street"),
    AddressRow("Minion 3", "The Street"),
    AddressRow("Darth Vader", "Death Star")
  )

  test("use join predicates") {
    val rdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("h_src")

    val queryString = """
    SELECT name, node FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H
                      """

    val hierarchy = sqlContext.sql(queryString)
    hierarchy.registerTempTable("h")

    val joinQuery =
      """
        |SELECT l.name, r.name, IS_DESCENDANT(l.node, r.node),
        | IS_DESCENDANT_OR_SELF(l.node, r.node), IS_PARENT(r.node, l.node)
        |FROM h l, h r
      """.stripMargin
    val result = sqlContext.sql(joinQuery).collect()

    val expectedPositives = Set(
      Row("The Other Middle Manager", "THE BOSS", true, true, true),
      Row("The Middle Manager", "THE BOSS", true, true, true),
      Row("Senior Developer", "THE BOSS", true, true, false),
      Row("Senior Developer", "The Middle Manager", true, true, true),
      Row("Minion 1", "THE BOSS", true, true, false),
      Row("Minion 1", "The Middle Manager", true, true, true),
      Row("Minion 2", "THE BOSS", true, true, false),
      Row("Minion 2", "The Middle Manager", true, true, false),
      Row("Minion 2", "Senior Developer", true, true, true),
      Row("Minion 3", "THE BOSS", true, true, false),
      Row("Minion 3", "The Middle Manager", true, true, false),
      Row("Minion 3", "Senior Developer", true, true, true)
    )
    val names = adjacencyList map (_.name)
    val allPairNames : Set[(String, String)] = (names cross names) toSet
    val positivePairNames : Set[(String, String)] =
      expectedPositives.map(r => r.getString(0) -> r.getString(1))
    val expectedNonPositives : Set[Row] = (allPairNames -- positivePairNames) map {
      case (left, right) if left == right => Row(left, right, false, true, false)
      case (left, right) => Row(left, right, false, false, false)
    }
    val expected = expectedPositives ++ expectedNonPositives
    val resultCollect = result.toSet
    if (expected != resultCollect) {
      log.error(s"Missing: ${expected -- resultCollect}")
      log.error(s"Unexpected: ${resultCollect -- expected}")
    }
    assertResult(expected)(resultCollect)
  }

  test("integration: build join hierarchy from SQL using RDD[Row] with UDFs") {
    val rdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT name, LEVEL(node), IS_ROOT(node) FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H
    """

    val result = sqlContext.sql(queryString).collect()

    val expected = Set(
      Row("THE BOSS", 1, true),
      Row("The Other Middle Manager", 2, false),
      Row("The Middle Manager", 2, false),
      Row("Senior Developer", 3, false),
      Row("Minion 1", 3, false),
      Row("Minion 2", 4, false),
      Row("Minion 3", 4, false)
    )

    assertResult(expected)(result.toSet)
  }

  test("integration: build join hierarchy top to bottom using SQL and RDD[Row]") {
    val rdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT * FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H
    """

    val result = sqlContext.sql(queryString).collect()

    val expected = Set(
      Row("THE BOSS", null, 1L, 1, Node(List(1L))),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L))),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L))),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L))),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L))),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L))),
      Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L)))
    )

    assertResult(expected)(result.toSet)
  }

  test("integration: build broadcast hierarchy top to bottom using SQL and RDD[Row]") {
    val rdd = sc.parallelize(
      adjacencyList
        .take(adjacencyList.length - 1)
        .sortBy(x => Random.nextDouble())
    )
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT * FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred IS NULL
      SET node
      ) AS H
    """

   val result = sqlContext.sql(queryString).collect()

   val expected = Set(
     Row("THE BOSS", null, 1L, 1, Node(List(1L))),
     Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L))),
     Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L))),
     Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L))),
     Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L))),
     Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L)))
   )

   assertResult(expected)(result.toSet)
  }

  integrationStartWithExpression(HierarchyRowJoinBuilder(
    Seq(
      AttributeReference("name", StringType)(),
      AttributeReference("pred", LongType, nullable = true)(),
      AttributeReference("succ", LongType)(),
      AttributeReference("ord", IntegerType)()
    ),
    EqualTo(
      AttributeReference("pred", LongType, nullable = true)(),
      AttributeReference("succ", LongType, nullable = false)()
    ),
    IsNull(null),
    Seq()
  ))

  integrationStartWithExpression(HierarchyRowBroadcastBuilder(
    Seq(
      AttributeReference("name", StringType)(),
      AttributeReference("pred", LongType, nullable = true)(),
      AttributeReference("succ", LongType)(),
      AttributeReference("ord", IntegerType)()
    ),
    EqualTo(
      AttributeReference("pred", LongType, nullable = true)(),
      AttributeReference("succ", LongType, nullable = false)()
    ),
    IsNull(null),
    Seq()
  ))

  def integrationStartWithExpression(builder : HierarchyBuilder[Row, Row]): Unit = {
    test("integration: execute hierarchy from expressions using " +
      builder.getClass.getName.split("\\$").head.split("\\.").last){
      val rdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
      val hSrc = sqlContext.createDataFrame(rdd)
      hSrc.registerTempTable("h_src")

      val result = builder.buildFromAdjacencyList(hSrc.rdd)

      val expected = Set(
        Row("THE BOSS", null, 1L, 1, Node(List(1L))),
        Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L))),
        Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L))),
        Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L))),
        Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L))),
        Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L))),
        Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L)))
      )

      assertResult(expected)(result.collect().toSet)
    }
  }

  buildFromAdjacencyListTest(HierarchyJoinBuilder(
    startWhere = (myRow: HierarchyRow) => myRow.pred.isEmpty,
    pk = (myRow: HierarchyRow) => myRow.succ,
    pred = (myRow: HierarchyRow) => myRow.pred.getOrElse(-1),
    init = (myRow: HierarchyRow) => PartialResult(pk = myRow.succ, path = Seq(myRow.succ)),
    modify = (pr: PartialResult, myRow: HierarchyRow) =>
      PartialResult(path = pr.path ++ Seq(myRow.succ), pk = myRow.succ)
  ))

  buildFromAdjacencyListTest(HierarchyBroadcastBuilder(
    pred = (myRow: HierarchyRow) => myRow.pred.getOrElse(-1),
    succ = (myRow: HierarchyRow) => myRow.succ,
    startWhere = (myRow: HierarchyRow) => myRow.pred.isEmpty,
    transformRowFunction = (r : HierarchyRow, node : Node) =>
      PartialResult(path = node.path.asInstanceOf[Seq[Long]], pk = r.succ)
  ))

  def buildFromAdjacencyListTest(builder : HierarchyBuilder[HierarchyRow, PartialResult]) {
    test("unitary: testing method buildFromAdjacencyList of class " +
      builder.getClass.getSimpleName){
       val rdd = sc.parallelize(adjacencyList)
       val hBuilder = HierarchyBroadcastBuilder(
         pred = (myRow: HierarchyRow) => myRow.pred.getOrElse(-1),
         succ = (myRow: HierarchyRow) => myRow.succ,
         startWhere = (myRow: HierarchyRow) => myRow.pred.isEmpty,
         transformRowFunction = (r : HierarchyRow, node : Node) =>
           PartialResult(path = node.path.asInstanceOf[Seq[Long]], pk = r.succ)
       )
       val hierarchy = builder.buildFromAdjacencyList(rdd)

       val expected = Set(
         PartialResult(List(1),1),
         PartialResult(List(1, 2),2),
         PartialResult(List(1, 3),3),
         PartialResult(List(1, 2, 4),4),
         PartialResult(List(1, 2, 5),5),
         PartialResult(List(1, 2, 4, 6),6),
         PartialResult(List(1, 2, 4, 7),7)
       )
       assertResult(expected)(hierarchy.collect().toSet)
    }
  }

  test("integration: I can join hierarchy with table") {
    val hRdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.error(s"tSrc: ${tRdd.collect().mkString("|")}")
    tSrc.registerTempTable("t_src")

    val queryString = """
      SELECT B.name, A.address, B.level
      FROM
      (SELECT name, LEVEL(node) AS level FROM HIERARCHY (
        USING h_src AS v
          JOIN PARENT u ON v.pred = u.succ
          SEARCH BY ord ASC
        START WHERE pred IS NULL
        SET node)
        AS H) B, t_src A
        WHERE B.name = A.name
    """
    val result = sqlContext.sql(queryString).collect()

    val expected = Set(
     Row("THE BOSS", "Nice Street", 1),
     Row("The Middle Manager", "Acceptable Street", 2),
     Row("Senior Developer", "Near-Acceptable Street", 3),
     Row("Minion 3", "The Street", 4)
    )
    assertResult(expected)(result.toSet)
   }

  test("integration: I can left outer join hierarchy with table") {
    val hRdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    tSrc.registerTempTable("t_src")

    val queryString = """
      SELECT A.name, B.address, LEVEL(A.node)
      FROM (
      SELECT * FROM HIERARCHY (
        USING h_src AS v
          JOIN PARENT u ON v.pred = u.succ
          SEARCH BY ord ASC
        START WHERE pred IS NULL
        SET node)
        AS H) A LEFT JOIN t_src B
        ON A.name = B.name
    """
    val result = sqlContext.sql(queryString).collect()

    val expected = Set(
     Row("THE BOSS", "Nice Street", 1),
     Row("The Other Middle Manager", null, 2),
     Row("The Middle Manager", "Acceptable Street", 2),
     Row("Senior Developer", "Near-Acceptable Street", 3),
     Row("Minion 1", null, 3),
     Row("Minion 2", null, 4),
     Row("Minion 3", "The Street", 4)
    )
    assertResult(expected)(result.toSet)
   }

  test("integration: I can right outer join hierarchy with table") {
    val hRdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.error(s"tSrc: ${tRdd.collect().mkString("|")}")
    tSrc.registerTempTable("t_src")

    val queryString = """
      SELECT A.name, A.address, B.level
      FROM
      (SELECT name, LEVEL(node) AS level FROM HIERARCHY (
        USING h_src AS v
          JOIN PARENT u ON v.pred = u.succ
          SEARCH BY ord ASC
        START WHERE pred IS NULL
        SET node)
        AS H) B RIGHT OUTER JOIN t_src A
        ON A.name = B.name
    """
    val result = sqlContext.sql(queryString).collect()

    val expected = Set(
     Row("THE BOSS", "Nice Street", 1),
     Row("The Middle Manager", "Acceptable Street", 2),
     Row("Senior Developer", "Near-Acceptable Street", 3),
     Row("Minion 3", "The Street", 4),
     Row("Darth Vader", "Death Star", null)
    )
    assertResult(expected)(result.toSet)
   }

    test("integration: I can full outer join hierarchy with table") {
    val hRdd = sc.parallelize(adjacencyList.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.error(s"tSrc: ${tRdd.collect().mkString("|")}")
    tSrc.registerTempTable("t_src")

    val queryString = """
      SELECT A.name, B.address, A.level
      FROM
      (SELECT name, LEVEL(node) AS level FROM HIERARCHY (
        USING h_src AS v
          JOIN PARENT u ON v.pred = u.succ
          SEARCH BY ord ASC
        START WHERE pred IS NULL
        SET node)
        AS H) A FULL OUTER JOIN t_src B
        ON A.name = B.name
    """
    val result = sqlContext.sql(queryString).collect()

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
}
