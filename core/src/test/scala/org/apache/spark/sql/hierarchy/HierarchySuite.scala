package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{IntegerType, LongType, Node, StringType}
import org.scalatest.FunSuite

import scala.util.Random

case class ComponentRow(extraField: String, name: String, pred: Long, succ: Long, ord: Long)

// scalastyle:off magic.number
// scalastyle:off file.size.limit
class HierarchySuite
  extends FunSuite
  with HierarchyTestUtils
  with GlobalSapSQLContext
  with Logging {

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X,Y)] =
      for { x <- xs; y <- ys } yield (x, y)
  }

  private def assertSetEqual[E](expected: Set[E])(actual: Set[E]): Unit = {
    val unexpectedElements = actual -- expected
    val missingElements = expected -- actual
    if (unexpectedElements.nonEmpty || missingElements.nonEmpty) {
      fail(
        s"""
           |Failed set comparison:
           |  Unexpected elements:
           |    $unexpectedElements
           |  Missing elements:
           |    $missingElements
         """.stripMargin)
    }
  }

  test("test hierarchy self join on hierarchy-UDF using repeated derivation") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val queryString =
      """
        |SELECT A.name, B.name
        |FROM
        |(SELECT name, node
        |  FROM HIERARCHY
        |     (USING hSrc AS v JOIN PARENT u ON v.pred = u.succ
        |     SEARCH BY ord ASC
        |     START WHERE pred IS NULL
        |     SET node) AS H) A,
        |(SELECT name, node
        |  FROM HIERARCHY
        |     (USING hSrc AS v JOIN PARENT u ON v.pred = u.succ
        |     SEARCH BY ord ASC
        |     START WHERE pred IS NULL
        |     SET node) AS H) B
        |WHERE IS_CHILD(A.node, B.node)
      """.stripMargin

    val hierarchy = sqlContext.sql(queryString).collect()

    val expected = Set(
      Row("The Other Middle Manager", "THE BOSS"),
      Row("The Middle Manager", "THE BOSS"),
      Row("Minion 1", "The Middle Manager"),
      Row("Minion 2", "Senior Developer"),
      Row("Minion 3", "Senior Developer"),
      Row("Senior Developer", "The Middle Manager")
    )

    assertResult(expected)(hierarchy.toSet)
  }

  test("use numerical startWhere predicate") {

    def parts: Seq[ComponentRow] = Seq(
      ComponentRow("bla", "mat-for-stuff",0L,1L,1L),
      ComponentRow("bla", "item-a-gen",1L,2L,2L),
      ComponentRow("bla", "item-o-piece",2L,3L,3L),
      ComponentRow("bla", "object-for-entity",3L,4L,4L),
      ComponentRow("bla", "whack-to-piece",3L,5L,5L),
      ComponentRow("bla", "gen-a-stuff",3L,6L,6L),
      ComponentRow("bla", "mat-with-whack",5L,7L,7L)
    )

    val rdd = sc.parallelize(parts.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("hSrc")

    val hierarchy = sqlContext.sql("""
                                     |SELECT name, node FROM HIERARCHY (
                                     |USING hSrc AS v
                                     |  JOIN PARENT u ON v.pred = u.succ
                                     |  SEARCH BY ord ASC
                                     |START WHERE pred = 0
                                     |SET node
                                     |) AS H
                                   """.stripMargin)
    hierarchy.registerTempTable("h")

    val result = sqlContext.sql("select name from h where IS_ROOT(node)").collect()

    val expected = Set(
      Row("mat-for-stuff")
    )

    assertResult(expected)(result.toSet)
  }

  test("hierarchy without any roots results in empty results") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("h_src")

    val queryString = """
    SELECT name, node FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE pred = 10000
      SET node
      ) AS H
    """

    val result = sqlc.sql(queryString).collect()
    assert(result.isEmpty)
  }

  test("create hierarchy without start where and search by clause") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd)
    log.error(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")
    val queryString = """
      | SELECT name, LEVEL(node), IS_ROOT(node) FROM HIERARCHY (
      |   USING h_src AS v
      |     JOIN PARENT u ON v.pred = u.succ
      |   SET node
      |   ) AS H""".stripMargin

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

  test("use join predicates") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
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
    val names = organizationHierarchy map (_.name)
    val allPairNames: Set[(String, String)] = (names cross names).toSet
    val positivePairNames: Set[(String, String)] =
      expectedPositives.map(r => r.getString(0) -> r.getString(1))
    val expectedNonPositives: Set[Row] = (allPairNames -- positivePairNames) map {
      case (left, right) if left == right => Row(left, right, false, true, false)
      case (left, right) => Row(left, right, false, false, false)
    }
    val expected = expectedPositives ++ expectedNonPositives
    val resultCollect = result.toSet
    if (expected != resultCollect) {
      log.error(s"Missing: ${expected -- resultCollect}")
      log.error(s"Unexpected: ${resultCollect -- expected}")
    }
    assertSetEqual(expected)(resultCollect)
  }

  test("integration: build join hierarchy from SQL using RDD[Row] with UDFs") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
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

    assertSetEqual(expected)(result.toSet)
  }

  test("integration: build join hierarchy top to bottom using SQL and RDD[Row]") {
    val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
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
      Row("THE BOSS", null, 1L, 1, Node(List(1L), 1, 7, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), 7, 6, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), 2, 5, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), 3, 3, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), 6, 4, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), 4, 1, isLeaf = true)),
      Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L), 5, 2, isLeaf = true))
    )

    assertSetEqual(expected)(result.toSet)
  }

  test("integration: build broadcast hierarchy top to bottom using SQL and RDD[Row]") {
    val rdd = sc.parallelize(
      organizationHierarchy
        .take(organizationHierarchy.length - 1)
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
      Row("THE BOSS", null, 1L, 1, Node(List(1L), 1, 6, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), 6, 5, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), 2, 4, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), 3, 2, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), 5, 3, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), 4, 1, isLeaf = true))
    )

    assertSetEqual(expected)(result.toSet)
  }

  test("integration: build broadcast hierarchy with orphan cycle") {
    val cyclingNodes =
      EmployeeRow("Cycler 1", Some(10L), 8L, 1) ::
        EmployeeRow("Cycler 2", Some(8L), 9L, 1) ::
        EmployeeRow("Cycler 3", Some(9L), 10L, 1) ::
      Nil

    val rdd = sc.parallelize(
      organizationHierarchy
        .take(organizationHierarchy.length - 1)
        .sortBy(x => Random.nextDouble()) ++ cyclingNodes
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
      Row("THE BOSS", null, 1L, 1, Node(List(1L), 1, 6, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), 6, 5, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), 2, 4, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), 3, 2, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), 5, 3, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), 4, 1, isLeaf = true))
    )

    assertSetEqual(expected)(result.toSet)
  }

  test("integration: build broadcast hierarchy with cycle") {
    val cycleHierarchy = Seq(
      EmployeeRow("Parent", Some(3L), 1L, 1),
      EmployeeRow("Child", Some(1L), 2L, 1),
      EmployeeRow("Cycler", Some(2L), 3L, 1)
    )
    val rdd = sc.parallelize(cycleHierarchy)
    val hSrc = sqlContext.createDataFrame(rdd).cache()
    hSrc.registerTempTable("h_src")
    val queryString = """
    SELECT * FROM HIERARCHY (
      USING h_src AS v
        JOIN PARENT u ON v.pred = u.succ
        SEARCH BY ord ASC
      START WHERE succ = 1
      SET node
      ) AS H
                      """

    val result = sqlContext.sql(queryString).collect().toSet

    val expected = Set(
      Row("Parent", 3L, 1L, 1, Node(List(1L), 1, 3, isLeaf = false)),
      Row("Child", 1L, 2L, 1, Node(List(1L, 2L), 2, 2, isLeaf = false)),
      Row("Cycler", 2L, 3L, 1, Node(List(1L, 2L, 3L), 3, 1, isLeaf = true))
    )

    assertSetEqual(expected)(result.toSet)
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
    IsNull(AttributeReference("pred", LongType, nullable = true)()),
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
    Some(IsNull(AttributeReference("pred", LongType, nullable = true)())),
    Seq()
  ))

  def integrationStartWithExpression(builder: HierarchyBuilder[Row, Row]): Unit = {
    test("integration: execute hierarchy from expressions using " +
      builder.getClass.getName.split("\\$").head.split("\\.").last){
      val rdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
      val hSrc = sqlContext.createDataFrame(rdd)
      hSrc.registerTempTable("h_src")

      val result = builder.buildFromAdjacencyList(hSrc.rdd)

      // TODO(Weidner): workaround, implement prerank for join builder!
      val isJoin = builder.getClass.getName.contains("JoinBuilder")
      val expected = if (isJoin) {
        Set(
          Row("THE BOSS", null, 1L, 1, Node(List(1L))),
          Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L))),
          Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L))),
          Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L))),
          Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L))),
          Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L))),
          Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L))))
      } else {
        Set(
          Row("THE BOSS", null, 1L, 1, Node(List(1L), 1, 7, isLeaf = false)),
          Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), 7, 6, isLeaf = true)),
          Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), 2, 5, isLeaf = false)),
          Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), 3, 3, isLeaf = false)),
          Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), 6, 4, isLeaf = true)),
          Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), 4, 1, isLeaf = true)),
          Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L), 5, 2, isLeaf = true)))
      }

      assertSetEqual(expected)(result.collect().toSet)
    }
  }

  buildFromAdjacencyListTest(HierarchyJoinBuilder(
    startWhere = (myRow: EmployeeRow) => myRow.pred.isEmpty,
    pk = (myRow: EmployeeRow) => myRow.succ,
    pred = (myRow: EmployeeRow) => myRow.pred.getOrElse(-1),
    init = (myRow: EmployeeRow, ordKey: Option[Long]) =>
      PartialResult(pk = myRow.succ, path = Seq(myRow.succ)),
    ord = (myRow: EmployeeRow) => myRow.ord,
    modify = (pr: PartialResult, myRow: EmployeeRow, ord) =>
      PartialResult(path = pr.path ++ Seq(myRow.succ), pk = myRow.succ)
  ))

  buildFromAdjacencyListTest(HierarchyBroadcastBuilder(
    pred = (myRow: EmployeeRow) => myRow.pred.getOrElse(-1),
    succ = (myRow: EmployeeRow) => myRow.succ,
    startWhere = Some((myRow: EmployeeRow) => myRow.pred.isEmpty),
    ord = (myRow: EmployeeRow) => myRow.ord,
    transformRowFunction = (r: EmployeeRow, node: Node) =>
      PartialResult(path = node.path.asInstanceOf[Seq[Long]], pk = r.succ)
  ))

  def buildFromAdjacencyListTest(builder: HierarchyBuilder[EmployeeRow, PartialResult]) {
    test("unitary: testing method buildFromAdjacencyList of class " +
      builder.getClass.getSimpleName){
      val rdd = sc.parallelize(organizationHierarchy)
      val hBuilder = HierarchyBroadcastBuilder(
        pred = (myRow: EmployeeRow) => myRow.pred.getOrElse(-1),
        succ = (myRow: EmployeeRow) => myRow.succ,
        startWhere = Some((myRow: EmployeeRow) => myRow.pred.isEmpty),
        ord = (myRow: EmployeeRow) => myRow.ord,
        transformRowFunction = (r: EmployeeRow, node: Node) =>
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

      val in_order = hierarchy.collect().toVector
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 3),3)) -
          in_order.indexOf(PartialResult(List(1, 2),2))
      )
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 2, 5),5)) -
          in_order.indexOf(PartialResult(List(1, 2, 4),4))
      )
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 2, 4, 7),7)) -
          in_order.indexOf(PartialResult(List(1, 2, 4, 6),6))
      )
    }
  }


  test("integration: I can join hierarchy with table") {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.info(s"tSrc: ${tRdd.collect().mkString("|")}")
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
    assertSetEqual(expected)(result.toSet)
  }

  test("integration: I can left outer join hierarchy with table") {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
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
    assertSetEqual(expected)(result.toSet)
   }

  test("integration: I can right outer join hierarchy with table") {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.info(s"tSrc: ${tRdd.collect().mkString("|")}")
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
    assertSetEqual(expected)(result.toSet)
  }

  test("integration: I can full outer join hierarchy with table") {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.info(s"tSrc: ${tRdd.collect().mkString("|")}")
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
    assertSetEqual(expected)(result.toSet)
  }

  test("integration: I can use star with full outer join hierarchy with table and unary UDFs") {
    val hRdd = sc.parallelize(organizationHierarchy.sortBy(x => Random.nextDouble()))
    val hSrc = sqlContext.createDataFrame(hRdd).cache()
    log.info(s"hSrc: ${hSrc.collect().mkString("|")}")
    hSrc.registerTempTable("h_src")

    val tRdd = sc.parallelize(addresses.sortBy(x => Random.nextDouble()))
    val tSrc = sqlContext.createDataFrame(tRdd).cache()
    log.info(s"tSrc: ${tRdd.collect().mkString("|")}")
    tSrc.registerTempTable("t_src")

    val queryString = """
      SELECT A.name, B.address, LEVEL(A.node), IS_ROOT(A.node)
      FROM
      (SELECT * FROM HIERARCHY (
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
      Row("THE BOSS", "Nice Street", 1, true),
      Row("The Other Middle Manager", null, 2, false),
      Row("The Middle Manager", "Acceptable Street", 2, false),
      Row("Senior Developer", "Near-Acceptable Street", 3, false),
      Row("Minion 1", null, 3, false),
      Row("Minion 2", null, 4, false),
      Row("Minion 3", "The Street", 4, false),
      Row(null, "Death Star", null, null)
    )
    assertSetEqual(expected)(result.toSet)
  }

  test("regression test for bug 92871") {
    val sRdd = sc.parallelize(sensors.sortBy(x => Random.nextDouble()))
    val sSrc = sqlContext.createDataFrame(sRdd).cache()
    sSrc.registerTempTable("sSrc")
    val result = sqlContext.sql(s"""
        | SELECT name FROM HIERARCHY ( USING sSrc AS v
        | JOIN PARENT u ON v.par = u.sensor SEARCH BY sensor ASC START WHERE sensor = "c"
        | SET node) AS H  WHERE IS_ROOT(node) = true""".stripMargin).collect()
    assertSetEqual(Set(
      Row("All Sensors")))(result.toSet)
  }
}
