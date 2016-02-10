package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.sources.sql.View
import org.apache.spark.sql.{AliasUnresolver, SQLContext}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands.{ShowTablesUsingCommand, CreatePartitioningFunction, DescribeDatasource}
import org.apache.spark.sql.types._
import org.mockito.Mockito
import org.scalatest.FunSuite
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}

/**
 * Suite for testing the SapDDLStrategy.
 */
class SapDDLStrategySuite extends FunSuite {
  test("DescribeDatasource with no actual relation") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val unresolved = UnresolvedRelation(Seq("test"))
    val describe = new DescribeDatasource(unresolved)

    Mockito.when(planner.optimizedRelationLookup(unresolved))
           .thenReturn(None)

    val command = strategy.apply(describe)
    assert(command == ExecutedCommand(DescribeDatasourceCommand(None)) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("DescribeDatasource with an actual relation") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val unresolved = UnresolvedRelation(Seq("test"))
    val describe = new DescribeDatasource(unresolved)
    val logical = Mockito.mock[LogicalRelation](classOf[LogicalRelation])

    val describable = new BaseRelation with DescribableRelation {
      override def sqlContext: SQLContext = null
      override def schema: StructType = null
      override def describe: Map[String, String] = Map.empty
    }


    Mockito.when(logical.relation).thenReturn(describable)
    Mockito.when(planner.optimizedRelationLookup(unresolved))
           .thenReturn(Some(logical))

    val command = strategy.apply(describe)
    assert(command == ExecutedCommand(
      DescribeDatasourceCommand(Some(describable))) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("CREATE PARTITION function test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val cpf = new CreatePartitioningFunction(Map[String, String](), "test",
      Seq(IntegerType, StringType), "hash", None, "com.sap.spark.vora")

    val command = strategy.apply(cpf)
    assert(command == ExecutedCommand(CreatePartitioningFunctionCommand(
      Map[String, String](), "test", Seq(IntegerType, StringType), "hash",
      None, "com.sap.spark.vora")) :: Nil)

    Mockito.validateMockitoUsage()
  }

  test("CREATE VIEW USING test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val unresolved = UnresolvedRelation(Seq("test"))
    val plan = unresolved.select('id)

    val viewCommand = CreatePersistentViewCommand(TableIdentifier("test"), plan,
      "com.sap.spark.vora", Map[String, String](), allowExisting = true)
    val command = strategy.apply(viewCommand)

    assert(command == ExecutedCommand(
      CreatePersistentViewRunnableCommand(TableIdentifier("test"), plan,
        "com.sap.spark.vora", Map[String, String](), allowExisting = true)) :: Nil)
    Mockito.validateMockitoUsage()
    Mockito.validateMockitoUsage()
  }

  test("DROP VIEW USING test") {
    val planner = Mockito.mock[ExtendedPlanner](classOf[ExtendedPlanner])
    val strategy = new SapDDLStrategy(planner)
    val unresolved = UnresolvedRelation(Seq("test"))
    val plan = unresolved.select('id)

    val viewCommand = DropPersistentViewCommand(TableIdentifier("test"), "com.sap.spark.vora",
      Map[String, String](), allowNotExisting = true)
    val command = strategy.apply(viewCommand)

    assert(command == ExecutedCommand(
      DropPersistentViewRunnableCommand(TableIdentifier("test"), "com.sap.spark.vora",
        Map[String, String](), allowNotExisting = true)) :: Nil)
    Mockito.validateMockitoUsage()
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
}
