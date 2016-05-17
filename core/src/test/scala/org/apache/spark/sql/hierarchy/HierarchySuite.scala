package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.{IsNull, EqualTo, AttributeReference}
import org.apache.spark.sql.types.Node
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.util.Random

// scalastyle:off magic.number
// scalastyle:off file.size.limit
class HierarchySuite
  extends FunSuite
  with BeforeAndAfter
  with HierarchyTestUtils
  with GlobalSapSQLContext
  with Logging {

  before {
    createOrgTable(sqlContext)
    createPartsTable(sqlContext)
    createAddressesTable(sqlContext)
  }

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]): Traversable[(X, Y)] =
      for { x <- xs; y <- ys } yield (x, y)
  }

  private def assertSetEqual[E](expected: Set[E])(actual: Set[E]): Unit = {
    val unexpectedElements = actual -- expected
    val missingElements = expected -- actual
    if (unexpectedElements.nonEmpty || missingElements.nonEmpty) {
      fail(s"""|Failed set comparison:
               |  Unexpected elements:
               |    $unexpectedElements
               |  Missing elements:
               |    $missingElements""".stripMargin)
    }
  }

  test("test hierarchy self join on hierarchy-UDF using repeated derivation") {
    val queryString =
      s"""
        |SELECT A.name, B.name
        |FROM
        |${adjacencyListHierarchySQL(orgTbl, "name, node")} A,
        |${adjacencyListHierarchySQL(orgTbl, "name, node")} B
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

    val hierarchy = sqlContext.sql(s"""|SELECT name, node FROM HIERARCHY (
                                      |USING $partsTable AS v
                                      |  JOIN PARENT u ON v.pred = u.succ
                                      |  SEARCH BY ord ASC
                                      |START WHERE pred = 0
                                      |SET node
                                      |) AS H""".stripMargin)
    hierarchy.registerTempTable("h")
    val result = sqlContext.sql("select name from h where IS_ROOT(node)").collect().toSet
    val expected = Set(Row("mat-for-stuff"))
    assertResult(expected)(result)
  }

  test("level based hierarchy works") {
    createLeveledOrgTable(sqlContext)

    val hierarchy = sqlContext.sql(levelBasedHierarchySQL(leveledOrgTbl, "col1, col2, col3," +
      " col4, node", 4))

    hierarchy.registerTempTable("h")
    val result = sqlContext.sql("SELECT col1 FROM h WHERE IS_ROOT(node)").collect.toSet
    assertResult(Set(Row("THE BOSS")))(result)
  }

  test("level-based hierarchy with numbers works correctly") {
    createNumericTable(sqlContext)

    val hierarchy = sqlContext.sql(levelBasedHierarchySQL(numericTbl, "col1, col2, col3, node", 3))

    hierarchy.registerTempTable("h")
    val result = sqlContext.sql("SELECT col1 FROM h WHERE IS_ROOT(node)").collect.toSet
    assertResult(Set(Row(1024)))(result)
  }

  test("hierarchy without any roots results in empty results") {
    val result = sqlc.sql(s"""SELECT name, node FROM HIERARCHY (
                              USING $orgTbl AS v
                                JOIN PARENT u ON v.pred = u.succ
                                SEARCH BY ord ASC
                              START WHERE pred = 10000
                              SET node
                              ) AS H""").collect()
    assert(result.isEmpty)
  }

  test("create hierarchy without start where and search by clause") {
    val result = sqlContext.sql(
      adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node), IS_ROOT(node)")).collect().toSet
    val expected = Set(
      Row("THE BOSS", 1, true),
      Row("The Other Middle Manager", 2, false),
      Row("The Middle Manager", 2, false),
      Row("Senior Developer", 3, false),
      Row("Minion 1", 3, false),
      Row("Minion 2", 4, false),
      Row("Minion 3", 4, false)
    )
    assertResult(expected)(result)
  }

  test("use join predicates") {
    sqlContext.sql(adjacencyListHierarchySQL(orgTbl, "name, node")).registerTempTable("h")
    val result = sqlContext.sql("""|SELECT l.name, r.name, IS_DESCENDANT(l.node, r.node),
                                   |IS_DESCENDANT_OR_SELF(l.node, r.node), IS_PARENT(r.node, l.node)
                                   |FROM h l, h r """.stripMargin).collect()
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
    val result = sqlContext.sql(
      adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node), IS_ROOT(node)")).collect().toSet
    val expected = Set(
      Row("THE BOSS", 1, true),
      Row("The Other Middle Manager", 2, false),
      Row("The Middle Manager", 2, false),
      Row("Senior Developer", 3, false),
      Row("Minion 1", 3, false),
      Row("Minion 2", 4, false),
      Row("Minion 3", 4, false)
    )
    assertSetEqual(expected)(result)
  }

  test("integration: build join hierarchy top to bottom using SQL and RDD[Row]") {
    val result = sqlContext.sql(adjacencyListHierarchySQL(orgTbl)).collect()
    val expected = Set(
      Row("THE BOSS", null, 1L, 1, Node(List(1L), LongType, 1, 7, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), LongType, 7, 6, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), LongType, 2, 5, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), LongType, 3, 3, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), LongType, 6, 4, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), LongType, 4, 1, isLeaf = true)),
      Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L), LongType, 5, 2, isLeaf = true))
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
    val result = sqlContext.sql(adjacencyListHierarchySQL("h_src")).collect()

    val expected = Set(
      Row("THE BOSS", null, 1L, 1, Node(List(1L), LongType, 1, 6, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), LongType, 6, 5, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), LongType, 2, 4, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), LongType, 3, 2, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), LongType, 5, 3, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), LongType, 4, 1, isLeaf = true))
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
    val result = sqlContext.sql(adjacencyListHierarchySQL("h_src")).collect()

    val expected = Set(
      Row("THE BOSS", null, 1L, 1, Node(List(1L), LongType, 1, 6, isLeaf = false)),
      Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), LongType, 6, 5, isLeaf = true)),
      Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), LongType, 2, 4, isLeaf = false)),
      Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), LongType, 3, 2, isLeaf = false)),
      Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), LongType, 5, 3, isLeaf = true)),
      Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), LongType, 4, 1, isLeaf = true))
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
    val result = sqlContext.sql("""SELECT * FROM HIERARCHY(
                                   USING h_src AS v JOIN PARENT u ON v.pred = u.succ
                                   SEARCH BY ord ASC
                                   START WHERE succ = 1
                                   SET node) AS H""").collect().toSet

    val expected = Set(
      Row("Parent", 3L, 1L, 1, Node(List(1L), LongType, 1, 3, isLeaf = false)),
      Row("Child", 1L, 2L, 1, Node(List(1L, 2L), LongType, 2, 2, isLeaf = false)),
      Row("Cycler", 2L, 3L, 1, Node(List(1L, 2L, 3L), LongType, 3, 1, isLeaf = true))
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

      val result = builder.buildHierarchyRdd(hSrc.rdd, LongType)

      // TODO(Weidner): workaround, implement prerank for join builder!
      val isJoin = builder.getClass.getName.contains("JoinBuilder")
      val expected = if (isJoin) {
        Set(
          Row("THE BOSS", null, 1L, 1, Node(List(1L), LongType)),
          Row("The Other Middle Manager", 1L, 3L, 2, Node(List(1L, 3L), LongType)),
          Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), LongType)),
          Row("Senior Developer", 2L, 4L, 1, Node(List(1L, 2L, 4L), LongType)),
          Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), LongType)),
          Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), LongType)),
          Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L), LongType)))
      } else {
        Set(
          Row("THE BOSS", null, 1L, 1, Node(List(1L), LongType, 1, 7, isLeaf = false)),
          Row("The Other Middle Manager", 1L, 3L, 2,
            Node(List(1L, 3L), LongType, 7, 6, isLeaf = true)),
          Row("The Middle Manager", 1L, 2L, 1, Node(List(1L, 2L), LongType, 2, 5, isLeaf = false)),
          Row("Senior Developer", 2L, 4L, 1,
            Node(List(1L, 2L, 4L), LongType, 3, 3, isLeaf = false)),
          Row("Minion 1", 2L, 5L, 2, Node(List(1L, 2L, 5L), LongType, 6, 4, isLeaf = true)),
          Row("Minion 2", 4L, 6L, 1, Node(List(1L, 2L, 4L, 6L), LongType, 4, 1, isLeaf = true)),
          Row("Minion 3", 4L, 7L, 2, Node(List(1L, 2L, 4L, 7L), LongType, 5, 2, isLeaf = true)))
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
    key = (myRow: EmployeeRow) => myRow.succ,
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
        key = (myRow: EmployeeRow) => myRow.succ,
        startWhere = Some((myRow: EmployeeRow) => myRow.pred.isEmpty),
        ord = (myRow: EmployeeRow) => myRow.ord,
        transformRowFunction = (r: EmployeeRow, node: Node) =>
          PartialResult(path = node.path.asInstanceOf[Seq[Long]], pk = r.succ)
      )
      val hierarchy = builder.buildHierarchyRdd(rdd, LongType)

      val expected = Set(
        PartialResult(List(1), 1),
        PartialResult(List(1, 2), 2),
        PartialResult(List(1, 3), 3),
        PartialResult(List(1, 2, 4), 4),
        PartialResult(List(1, 2, 5), 5),
        PartialResult(List(1, 2, 4, 6), 6),
        PartialResult(List(1, 2, 4, 7), 7)
      )
      assertResult(expected)(hierarchy.collect().toSet)

      val in_order = hierarchy.collect().toVector
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 3), 3)) -
          in_order.indexOf(PartialResult(List(1, 2), 2))
      )
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 2, 5), 5)) -
          in_order.indexOf(PartialResult(List(1, 2, 4), 4))
      )
      assertResult(1)(  // should follow in order
        in_order.indexOf(PartialResult(List(1, 2, 4, 7), 7)) -
          in_order.indexOf(PartialResult(List(1, 2, 4, 6), 6))
      )
    }
  }

  test("integration: I can join hierarchy with table") {
    val result = sqlContext.sql(s"""SELECT B.name, A.address, B.level
      FROM ${adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node) AS level")} B,
      $addressesTable A
      WHERE B.name = A.name""").collect().toSet

    val expected = Set(
      Row("THE BOSS", "Nice Street", 1),
      Row("The Middle Manager", "Acceptable Street", 2),
      Row("Senior Developer", "Near-Acceptable Street", 3),
      Row("Minion 3", "The Street", 4)
    )
    assertSetEqual(expected)(result)
  }

  test("integration: I can left outer join hierarchy with table") {
    val result = sqlContext.sql(s"""SELECT A.name, B.address, A.level
      FROM ${adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node) AS level")} A
      LEFT OUTER JOIN $addressesTable B
      ON A.name = B.name""").collect().toSet

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
    val result = sqlContext.sql(s"""SELECT A.name, A.address, B.level
      FROM ${adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node) AS level")} B
      RIGHT OUTER JOIN $addressesTable A
      ON A.name = B.name""").collect().toSet

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
    val result = sqlContext.sql(s"""SELECT A.name, B.address, A.level
      FROM ${adjacencyListHierarchySQL(orgTbl, "name, LEVEL(node) AS level")} A
      FULL OUTER JOIN $addressesTable B
      ON A.name = B.name""").collect().toSet

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
    assertSetEqual(expected)(result)
  }

  test("integration: I can use star with full outer join hierarchy with table and unary UDFs") {
    val result = sqlContext.sql(s"""
                    SELECT A.name, B.address, LEVEL(A.node), IS_ROOT(A.node)
                    FROM ${adjacencyListHierarchySQL(orgTbl)} A FULL OUTER JOIN $addressesTable B
                    ON A.name = B.name""").collect()

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
    createSensorsTable(sqlContext)
    val result = sqlContext.sql(s"""
        |SELECT name FROM HIERARCHY ( USING $sensorsTable AS v
        |JOIN PARENT u ON v.par = u.sensor
        |SEARCH BY sensor ASC
        |START WHERE sensor = "c"
        |SET node) AS H
        |WHERE IS_ROOT(node) = true""".stripMargin).collect().toSet
    assertSetEqual(Set(Row("All Sensors")))(result)
  }

  test("one level hierarchy works correctly") {
    val rdd = sqlContext.sparkContext.parallelize(Seq(Row("row1"), Row("row2")))
    val df = sqlContext.createDataFrame(
      rdd,
      new StructType((new StructField("col1", StringType) :: Nil).toArray))
    sqlContext.registerDataFrameAsTable(df, "t1")

    val hierarchy = sqlContext.sql(levelBasedHierarchySQL("t1", "col1, node", 1))
    sqlContext.registerDataFrameAsTable(hierarchy, "h")

    val result = sqlContext.sql("SELECT NAME(node), col1, LEVEL(node) FROM h").collect()
    assertResult(Set(Row("row1", "row1", 1), Row("row2", "row2", 1)))(result.toSet)
  }

  test("one level and one row hierarchy works correctly") {
    val rdd = sqlContext.sparkContext.parallelize(Seq(Row("row1")))
    val df = sqlContext.createDataFrame(
      rdd,
      new StructType((new StructField("col1", StringType) :: Nil).toArray))
    sqlContext.registerDataFrameAsTable(df, "t1")

    val hierarchy = sqlContext.sql(levelBasedHierarchySQL("t1", "col1, node", 1))
    sqlContext.registerDataFrameAsTable(hierarchy, "h")

    val result = sqlContext.sql("SELECT NAME(node), col1, LEVEL(node) FROM h").collect()
    assertResult(Set(Row("row1", "row1", 1)))(result.toSet)
  }

  test("a hierarchy with empty table works correctly") {
    val rdd = sqlContext.sparkContext.emptyRDD[Row]
    val df = sqlContext.createDataFrame(
      rdd,
      new StructType((new StructField("col1", StringType) :: Nil).toArray))
    sqlContext.registerDataFrameAsTable(df, "t1")

    val hierarchy = sqlContext.sql(levelBasedHierarchySQL("t1", "col1, node", 1))
    sqlContext.registerDataFrameAsTable(hierarchy, "h")

    val result = sqlContext.sql("SELECT NAME(node), col1, LEVEL(node) FROM h").collect()
    assertResult(Set.empty)(result.toSet)
  }
}
