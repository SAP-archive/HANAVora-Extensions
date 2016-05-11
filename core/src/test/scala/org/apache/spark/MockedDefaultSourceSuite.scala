package org.apache.spark

import java.util.concurrent.{Callable, Executors}

import com.sap.spark.dsmock.DefaultSource
import org.apache.spark.sql.sources.HashPartitioningFunction
import org.apache.spark.sql.{GlobalSapSQLContext, Row, SQLContext}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.FunSuite

import scala.concurrent.duration._

/**
  * Test suite to verify behavior of the mockable [[DefaultSource]].
  */
class MockedDefaultSourceSuite
  extends FunSuite
  with GlobalSapSQLContext {

  val testTimeout = 10 // seconds

  private def numberOfThreads: Int = {
    val noOfCores = Runtime.getRuntime.availableProcessors()
    assert(noOfCores > 0)

    if (noOfCores == 1) 2 // It should always be multithreaded although only
                          // one processor is available (pseudo-multithreading)
    else noOfCores
  }

  def runMultiThreaded[A](op: Int => A): Seq[A] = {
    info(s"Running with $numberOfThreads threads")
    val pool = Executors.newFixedThreadPool(numberOfThreads)

    val futures = 1 to numberOfThreads map { i =>
      val task = new Callable[A] {
        override def call(): A = op(i)
      }
      pool.submit(task)
    }

    futures.map(_.get(testTimeout, SECONDS))
  }

  test("Underlying mocks of multiple threads are distinct") {
    val dataSources = runMultiThreaded { _ =>
      DefaultSource.withMock(identity)
    }

    dataSources foreach { current =>
      val sourcesWithoutCurrent = dataSources.filter(_.ne(current))
      assert(sourcesWithoutCurrent.forall(_.underlying ne current))
    }
  }

  test("Mocking works as expected") {
    runMultiThreaded { i =>
      DefaultSource.withMock { defaultSource =>
        when(defaultSource.getAllPartitioningFunctions(
          anyObject[SQLContext],
          anyObject[Map[String, String]]))
          .thenReturn(Seq(HashPartitioningFunction(s"foo$i", Seq.empty, None)))

        val Array(Row(name)) = sqlc
          .sql("SHOW PARTITION FUNCTIONS USING com.sap.spark.dsmock")
          .select("name")
          .collect()

        assertResult(s"foo$i")(name)
      }
    }
  }
}
