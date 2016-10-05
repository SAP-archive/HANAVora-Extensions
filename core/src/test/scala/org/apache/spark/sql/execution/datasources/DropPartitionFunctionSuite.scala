package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.DatasourceResolver._
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.mockito.Mockito._
import org.mockito.internal.stubbing.answers.Returns
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

/**
  * The [[DropPartitioningFunctionCommand]] supports to base traits to drop partition functions:
  * - [[org.apache.spark.sql.sources.PartitionCatalog]]
  * - [[org.apache.spark.sql.sources.PartitioningFunctionProvider]]
  *
  * This test suite test if the distinction works.
  *
  */
class DropPartitionFunctionSuite extends FunSuite
  with GlobalSapSQLContext
  with MockitoSugar {

  test("PartitionCatalog drop") {
    val pfName = "pf"

    val catalog = mock[PartitionCatalog]

    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("foo")).thenAnswer(new Returns(catalog))

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql(
          s"""DROP PARTITION FUNCTION $pfName
              |USING foo""".stripMargin)
          .collect()

      verify(catalog, times(1)).dropPartitionFunction(pfName)
    }
  }

  test("PartitioningFunctionProvider drop") {
    val pfName = "pf"

    val catalog = mock[PartitioningFunctionProvider]

    val resolver = mock[DatasourceResolver]
    when(resolver.newInstanceOf("foo")).thenAnswer(new Returns(catalog))

    withResolver(sqlContext, resolver) {
      val values =
        sqlc.sql(
          s"""DROP PARTITION FUNCTION $pfName
              |USING foo""".stripMargin)
          .collect()

      verify(catalog, times(1)).dropPartitioningFunction(sqlContext, Map.empty, pfName, false)
    }
  }
}
