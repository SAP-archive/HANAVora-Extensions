package org.apache.spark.sql

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.commands.{Table, View, WithExplicitRelationKind}
import org.apache.spark.sql.sources.{BaseRelation, DropRelation}
import org.apache.spark.sql.types.StructType
import org.scalatest.FunSuite

class DropCommandSuite extends FunSuite with GlobalSapSQLContext {
  val persons =
    Person("John", 20) :: Person("Bill", 30) :: Nil // scalastyle:ignore magic.number

  lazy val existingTable1 = tableName("existingTable1")
  lazy val existingTable2 = tableName("existingTable2")
  lazy val willBeDeleted = tableName("willBeDeleted")
  lazy val someTable = tableName("someTable")
  lazy val someTable2 = tableName("someTable2")
  lazy val someView = tableName("someView")
  lazy val someOtherView = tableName("someOtherView")

  test("Drop a Spark table referencing Vora tables removes it from Spark catalog."){
    sqlContext.sql(s"""CREATE TABLE $existingTable1 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $existingTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE $willBeDeleted
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(existingTable1, false)))
    assert(result.contains(Row(existingTable2, true)))
    assert(result.contains(Row(willBeDeleted, false)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE $willBeDeleted")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 2)
    assert(result2.contains(Row(existingTable1, false)))
    assert(result2.contains(Row(existingTable2, true)))
    assert(!result2.contains(Row(willBeDeleted, false)))
  }

  test("Drop Spark table succeeds if it does not exist but if exists flag is provided") {
    sqlContext
      .sql(s"DROP TABLE IF EXISTS $someTable")
  }

  test("Drop Spark table succeeds if it does exist and if exists flag is provided") {
    sqlContext.sql(s"""CREATE TABLE $existingTable1 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $existingTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TABLE $willBeDeleted
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(existingTable1, false)))
    assert(result.contains(Row(existingTable2, true)))
    assert(result.contains(Row(willBeDeleted, false)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE IF EXISTS $willBeDeleted")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 2)
    assert(result2.contains(Row(existingTable1, false)))
    assert(result2.contains(Row(existingTable2, true)))
    assert(!result2.contains(Row(willBeDeleted, false)))
  }

  test("Drop Spark table fails if it is referenced more than once in catalog."){
    val ex = intercept[AnalysisException] {
      sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE TEMPORARY TABLE $someTable2 (id string)
                        |USING com.sap.spark.dstest
                        |OPTIONS ()""".stripMargin)

      sqlContext.sql(s"""CREATE TEMPORARY VIEW $someView
                        |AS SELECT * FROM $someTable""".stripMargin)

      val result = sqlContext.tables().collect()
      assert(result.contains(Row(someTable, false)))
      assert(result.contains(Row(someTable2, true)))
      assert(result.contains(Row(someView, true)))
      assert(result.length == 3)

      sqlContext.sql(s"DROP TABLE $someTable")
    }

    assert(ex.getMessage.contains("Can not drop because more than one relation has " +
      "references to the target relation"))
  }

  test("Drop Spark table succeeds if it referenced more than once in catalog" +
    "with cascading") {
    sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY TABLE $someTable2 (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE TEMPORARY VIEW $someView
                      |AS SELECT * FROM $someTable""".stripMargin)

    val result = sqlContext.tables().collect()
    assert(result.contains(Row(someTable, false)))
    assert(result.contains(Row(someTable2, true)))
    assert(result.contains(Row(someView, true)))
    assert(result.length == 3)

    sqlContext.sql(s"DROP TABLE $someTable CASCADE")

    val result2 = sqlContext.tables().collect()
    assert(result2.length == 1)
    assert(result2.contains(Row(someTable2, true)))
  }

  test("Drop table cascade also drops related views (Bug 106016)") {
    sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW $someView
                      |AS SELECT * FROM $someTable
                      |USING com.sap.spark.dstest""".stripMargin)

    sqlContext.sql(s"DROP TABLE $someTable cascade")

    assert(!sqlContext.catalog.tableExists(TableIdentifier(someTable)))
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someView)))
  }

  test("Drop view cascade drops dependent relations (Bug 106016), (Bug 107567)") {
    sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW $someView
                      |AS SELECT * FROM $someTable
                      |USING com.sap.spark.dstest""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW $someOtherView
                      |AS SELECT * FROM $someView
                      |USING com.sap.spark.dstest""".stripMargin)

    sqlContext.sql(s"DROP VIEW $someView cascade")

    assert(sqlContext.catalog.tableExists(TableIdentifier(someTable)))
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someView)))
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someOtherView)))
  }

  test("Drop single view works (Bug 107566)") {
    sqlContext.sql(s"""CREATE TABLE $someTable (id string)
                      |USING com.sap.spark.dstest
                      |OPTIONS ()""".stripMargin)

    sqlContext.sql(s"""CREATE VIEW $someView
                      |AS SELECT * FROM $someTable""".stripMargin)

    sqlContext.sql(s"DROP VIEW $someView")

    assert(sqlContext.catalog.tableExists(TableIdentifier(someTable)))
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someView)))
  }

  trait DropDummy extends DropRelation {
    this: WithExplicitRelationKind =>

    var wasDropped = false

    override def dropTable(): Unit = wasDropped = true
  }

  class DummyDroppableTable extends BaseRelation with DropDummy with Table {
    override def sqlContext: SQLContext = sqlc

    override def schema: StructType = StructType(Seq())
  }

  case class DummyDroppableView(reference: String) extends UnaryNode with DropDummy with View {
    override val child: LogicalPlan = UnresolvedRelation(TableIdentifier(reference))

    override def output: Seq[Attribute] = Seq.empty
  }

  test("Drop actually executes drop on the target relation if it is droppable") {
    val relation = new DummyDroppableTable
    val logicalRelation = new LogicalRelation(relation)
    sqlContext.registerRawPlan(logicalRelation, someTable)

    sqlContext.sql(s"DROP TABLE $someTable")

    assert(relation.wasDropped)
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someTable)))
  }

  test("Drop fails on a table that does not exist in the catalog") {
    intercept[AnalysisException] {
      sqlContext.sql(s"DROP TABLE $someTable")
    }
  }

  test("Drop cascade will also drop the referencing relations") {
    val relation = new DummyDroppableTable
    val logicalRelation = new LogicalRelation(relation)
    val view = new DummyDroppableView(someTable)
    sqlContext.registerRawPlan(logicalRelation, someTable)
    sqlContext.registerRawPlan(view, someView)

    sqlContext.sql(s"DROP TABLE $someTable CASCADE")

    assert(relation.wasDropped)
    assert(view.wasDropped)
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someTable)))
    assert(!sqlContext.catalog.tableExists(TableIdentifier(someView)))
  }

  test("Drop KIND will throw if the target is not of the specified kind") {
    val relation = new DummyDroppableTable
    val logicalRelation = new LogicalRelation(relation)
    val view = new DummyDroppableView(someTable)
    sqlContext.registerRawPlan(logicalRelation, someTable)
    sqlContext.registerRawPlan(view, someView)

    intercept[AnalysisException] {
      sqlContext.sql(s"DROP VIEW $someTable")
    }

    intercept[AnalysisException] {
      sqlContext.sql(s"DROP TABLE $someView")
    }
  }

  test("DROP VIEW will throw if executed on a spark table") {
    val rdd = sc.parallelize(persons)
    val df = sqlContext.createDataFrame(rdd)
    sqlContext.registerDataFrameAsTable(df, someTable)
    assert(sqlContext.catalog.tableExists(TableIdentifier(someTable)))

    intercept[AnalysisException] {
      sqlc.sql(s"DROP VIEW $someTable")
    }
  }

  test("DROP TABLE will drop a single affected spark table") {
    val rdd = sc.parallelize(persons)
    val df = sqlContext.createDataFrame(rdd)
    sqlContext.registerDataFrameAsTable(df, someTable)
    assert(sqlContext.catalog.tableExists(TableIdentifier(someTable)))

    sqlc.sql(s"DROP TABLE $someTable")

    assert(!sqlContext.catalog.tableExists(TableIdentifier(someTable)))
  }
}

private[sql] case class Person(name: String, age: Int)
