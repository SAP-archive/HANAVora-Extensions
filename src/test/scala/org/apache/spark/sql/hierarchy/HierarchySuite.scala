package org.apache.spark.sql.hierarchy

import corp.sap.spark.SharedSparkContext
import org.apache.spark.{SparkConf, Logging}
import org.apache.spark.sql.types.Node
import org.apache.spark.sql.execution.{SparkPlan, HierarchyPhysicalPlan}
import org.apache.spark.sql.hierarchy._
import org.apache.spark.sql.types.{IntegerType, StringType, LongType}
import org.apache.spark.sql.{VelocitySQLContext, DataFrameHolder, SQLContext, Row}
import org.apache.spark.sql.catalyst.expressions._
import org.scalatest.FunSuite

import scala.util.Random

case class MyRow(name : String, pred : Option[Long], succ : Long, ord : Int)

case class PartialResult(path: Seq[Long], pk: Long)

// scalastyle:off magic.number
// scalastyle:off file.size.limit

class HierarchySuite extends FunSuite with SharedSparkContext with Logging {

  override def sparkConf : SparkConf = new SparkConf(false)

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) : Traversable[(X,Y)] =
      for { x <- xs; y <- ys } yield (x, y)
  }

  def adjacencyList : Seq[MyRow] = Seq(
    MyRow("THE BOSS", None, 1L, 1),
    MyRow("The Middle Manager", Some(1L), 2L, 1),
    MyRow("The Other Middle Manager", Some(1L), 3L, 2),
    MyRow("Senior Developer", Some(2L), 4L, 1),
    MyRow("Minion 1", Some(2L), 5L, 2),
    MyRow("Minion 2", Some(4L), 6L, 1),
    MyRow("Minion 3", Some(4L), 7L, 2)
  )

  test("use join predicates") {
    val sqlContext = new VelocitySQLContext(sc)
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
    val result = sqlContext.sql(joinQuery).cache()

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
    val resultCollect = result.collect().toSet
    if (expected != resultCollect) {
      log.error(s"Missing: ${expected -- resultCollect}")
      log.error(s"Unexpected: ${resultCollect -- expected}")
    }
    assertResult(expected)(resultCollect)
  }

  test("integration: build join hierarchy from SQL using RDD[Row] with UDFs") {
    val sqlContext = new VelocitySQLContext(sc)
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

    val result = sqlContext.sql(queryString).cache()

    val expected = Set(
      Row("THE BOSS", 1, true),
      Row("The Other Middle Manager", 2, false),
      Row("The Middle Manager", 2, false),
      Row("Senior Developer", 3, false),
      Row("Minion 1", 3, false),
      Row("Minion 2", 4, false),
      Row("Minion 3", 4, false)
    )

    assertResult(expected)(result.collect().toSet)
  }

  test("integration: build join hierarchy top to bottom using SQL and RDD[Row]") {
    val sqlContext = new VelocitySQLContext(sc)
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

    val result = sqlContext.sql(queryString).cache()

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

  test("integration: build broadcast hierarchy top to bottom using SQL and RDD[Row]") {
    val sqlContext = new VelocitySQLContext(sc)
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

   val result = sqlContext.sql(queryString).cache()

   val expected = Set(
     Row("THE BOSS", null, 1L, 1, Node(List(1L))),
     Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L))),
     Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L))),
     Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L))),
     Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L))),
     Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L)))
   )

   assertResult(expected)(result.collect().toSet)
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
      val sqlContext = new VelocitySQLContext(sc)
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
    startWhere = (myRow: MyRow) => myRow.pred.isEmpty,
    pk = (myRow: MyRow) => myRow.succ,
    pred = (myRow: MyRow) => myRow.pred.getOrElse(-1),
    init = (myRow: MyRow) => PartialResult(pk = myRow.succ, path = Seq(myRow.succ)),
    modify = (pr: PartialResult, myRow: MyRow) =>
      PartialResult(path = pr.path ++ Seq(myRow.succ), pk = myRow.succ)
  ))

  buildFromAdjacencyListTest(HierarchyBroadcastBuilder(
    pred = (myRow: MyRow) => myRow.pred.getOrElse(-1),
    succ = (myRow: MyRow) => myRow.succ,
    startWhere = (myRow: MyRow) => myRow.pred.isEmpty,
    transformRowFunction = (r : MyRow, node : Node) =>
      PartialResult(path = node.path.asInstanceOf[Seq[Long]], pk = r.succ)
  ))

  def buildFromAdjacencyListTest(builder : HierarchyBuilder[MyRow, PartialResult]) {
    test("unitary: testing method buildFromAdjacencyList of class " +
      builder.getClass.getSimpleName){
       val rdd = sc.parallelize(adjacencyList)
       val hBuilder = HierarchyBroadcastBuilder(
         pred = (myRow: MyRow) => myRow.pred.getOrElse(-1),
         succ = (myRow: MyRow) => myRow.succ,
         startWhere = (myRow: MyRow) => myRow.pred.isEmpty,
         transformRowFunction = (r : MyRow, node : Node) =>
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

}
