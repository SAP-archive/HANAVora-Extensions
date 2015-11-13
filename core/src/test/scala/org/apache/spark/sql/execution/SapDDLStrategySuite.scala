package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.execution.datasources.{DescribeDatasourceCommand, LogicalRelation}
import org.apache.spark.sql.extension.ExtendedPlanner
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.commands.DescribeDatasource
import org.apache.spark.sql.types.compat._
import org.mockito.Mockito
import org.scalatest.FunSuite

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
}
