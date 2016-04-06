package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources.commands._
import org.apache.spark.sql.types._
import org.mockito.Mockito
import org.scalatest.FunSuite

/**
 * Suite for testing the SapDDLStrategy.
 */
class SapDDLStrategySuite extends FunSuite {
  test("CREATE HASH PARTITION function test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val cpf = new CreateHashPartitioningFunction(Map[String, String](), "test",
      "com.sap.spark.vora", Seq(IntegerType, StringType), None)

    val command = strategy.apply(cpf)
    assert(command == ExecutedCommand(CreateHashPartitioningFunctionCommand(
      Map[String, String](), "test", Seq(IntegerType, StringType),
      None, "com.sap.spark.vora")) :: Nil)

    Mockito.validateMockitoUsage()
  }

  // scalastyle:off magic.number
  test("CREATE RANGE SPLIT PARTITION function test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val cpf = new CreateRangeSplittersPartitioningFunction(Map[String, String](), "test",
      "com.sap.spark.vora", IntegerType, Seq(5, 10), false)

    val command = strategy.apply(cpf)
    assert(command == ExecutedCommand(CreateRangeSplitPartitioningFunctionCommand(
      Map[String, String](), "test", IntegerType, Seq(5, 10), rightClosed = false,
      "com.sap.spark.vora")) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("CREATE RANGE INTERVAL PARTITION function test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val cpf = new CreateRangeIntervalPartitioningFunction(Map[String, String](), "test",
      "com.sap.spark.vora", IntegerType, 0, 100, Left(1))

    val command = strategy.apply(cpf)
    assert(command == ExecutedCommand(CreateRangeIntervalPartitioningFunctionCommand(
      Map[String, String](), "test", IntegerType,
      0, 100, Left(1), "com.sap.spark.vora")) :: Nil)

    Mockito.validateMockitoUsage()
  }
  // scalastyle:on magic.number

  test("DROP PARTITION function test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val dpf = new DropPartitioningFunction(Map.empty, "test", "com.sap.spark.vora",
      false)

    val command = strategy.apply(dpf)
    assert(command == ExecutedCommand(DropPartitioningFunctionCommand(Map.empty,
      "test", allowNotExisting = false, "com.sap.spark.vora")) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("SHOW TABLES USING test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val showTables = new ShowTablesUsingCommand("com.sap.spark.vora", Map[String, String]())

    val command = strategy.apply(showTables)
    assert(command == ExecutedCommand(ShowTablesUsingRunnableCommand("com.sap.spark.vora",
      Map[String, String]())) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("DESCRIBE TABLE USING test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val unresolved = UnresolvedRelation(Seq("test"))
    val plan = unresolved.select('id)

    val viewCommand = DescribeTableUsingCommand("test" :: Nil, "com.sap.spark.vora", Map.empty)
    val command = strategy.apply(viewCommand)

    assert(command == ExecutedCommand(
      DescribeTableUsingRunnableCommand("test" :: Nil, "com.sap.spark.vora", Map.empty)) :: Nil)
    Mockito.validateMockitoUsage()
    Mockito.validateMockitoUsage()
  }
}
