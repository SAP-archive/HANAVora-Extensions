package org.apache.spark.sql.sources

import com.sap.spark.dsmock.DefaultSource
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.SelectWith
import org.apache.spark.sql.execution.datasources.RawSqlSourceStrategy
import org.apache.spark.sql.execution.{PhysicalRDD, SparkPlan}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class RawSqlSourceStrategySuite extends FunSuite
  with GlobalSapSQLContext
  with BeforeAndAfterEach {

  def testWithMockedSource(block: => Unit): Unit = {
    DefaultSource.withMock{ defaultSource =>
      when(defaultSource.getResultingAttributes(
        anyObject[String]))
        .thenReturn(attributes)
      when(defaultSource.getRDD(sqlCommand))
        .thenReturn(new DummyRDD(sqlCommand, SparkContext.getOrCreate()))

      block
    }
  }

  val className = "com.sap.spark.dsmock"
  val sqlCommand = "SELECT DFT(*) FROM TABLE"
  val attributes = Seq(AttributeReference("a", IntegerType)(ExprId(0)),
    AttributeReference("b", StringType)(ExprId(1)))

  test("Strategy transforms correctly") {
    testWithMockedSource {
      val logicalPlan = SelectWith(sqlCommand, className, attributes)

      val physicalPlan: SparkPlan = RawSqlSourceStrategy.apply(logicalPlan).head

      checkPhysicalPlan(physicalPlan,
        "org.apache.spark.sql.sources.RawSqlSourceStrategySuite.DummyRDD",
        (rdd => rdd.asInstanceOf[DummyRDD].sqlCommand),
        sqlCommand,
        attributes)
    }
  }

  test("Error is thrown if data source does not implement the RawSqlProvider Interface"){
    testWithMockedSource {
      val logicalPlan = SelectWith(sqlCommand, "none.existant.class", attributes)

      intercept[ClassNotFoundException](RawSqlSourceStrategy.apply(logicalPlan).head)
    }
  }

  /**
    * This test is supposed to test the 'full stack' from parsing, over physical planning
    */
  test("Full Stack Test"){
    testWithMockedSource {
      val df = sqlc.sql(s"""'${sqlCommand}' WITH ${className}""")

      assert(df.logicalPlan == SelectWith(sqlCommand, className, attributes))

      checkPhysicalPlan(df.queryExecution.executedPlan,
        "org.apache.spark.sql.sources.RawSqlSourceStrategySuite.DummyRDD",
        (rdd => rdd.asInstanceOf[DummyRDD].sqlCommand),
        sqlCommand,
        attributes)
    }
  }

  /**
    *
    * @param physicalPlan has to be the Physical RDD produced by [[RawSqlSourceStrategy]]
    * @param expectedClassName
    * @param extractSQLCommand function to get the sql from the planned RDD
    * @param sqlCommand
    * @param attributes
    */
  private def checkPhysicalPlan(physicalPlan: SparkPlan,
                                expectedClassName: String,
                                extractSQLCommand: RDD[_] => String,
                                sqlCommand: String,
                                attributes: Seq[Attribute]): Unit = {

    // extract parts of the physical plan
    assert(physicalPlan.isInstanceOf[PhysicalRDD])
    val physicalRDD = physicalPlan.asInstanceOf[PhysicalRDD]
    val (oneToOne: OneToOneDependency[Row]) :: _ = physicalRDD.rdd.dependencies
    val plannedRdd = oneToOne.rdd

    assert(plannedRdd.getClass.getCanonicalName == expectedClassName)
    assert(extractSQLCommand(plannedRdd) == sqlCommand)
    assert(physicalRDD.output == attributes)
  }


  /**
    * RDD to return for this test data source, implementation not needed since never used
    *
    * @param sc
    */
  class DummyRDD (val sqlCommand: String, sc: SparkContext)
    extends RDD[Row](sc, Nil) {

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[Row] =
      throw new NotImplementedError("Dummy RDD should be used only for tests, not for execution")

    override protected def getPartitions: Array[Partition] =
      throw new NotImplementedError("Dummy RDD should be used only for tests, not for execution")
  }

}
