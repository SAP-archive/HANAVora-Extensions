package org.apache.spark

import com.sap.spark.dstest.DefaultSource
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, GlobalSapSQLContext}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class DatasourceResolverSuite extends FunSuite with GlobalSapSQLContext with MockitoSugar {
  test("DataSourceResolver looks up classes correclty") {
    val clazz = new DefaultDatasourceResolver().lookup("com.sap.spark.dstest")
    assertResult(classOf[DefaultSource])(clazz)
  }

  test("DefaultDatasourceResolver creates a new instance of a given provider correclty") {
    val instance =
      new DefaultDatasourceResolver().newInstanceOfTyped[DefaultSource]("com.sap.spark.dstest")
    assert(instance.isInstanceOf[DefaultSource])
  }

  test("withResolver uses the correct resolver") {
    val resolver = mock[DatasourceResolver]
    DatasourceResolver.withResolver(sqlContext, resolver) {
      assertResult(DatasourceResolver.resolverFor(sqlContext))(resolver)
    }
    assertResult(DatasourceResolver.resolverFor(sqlContext))(DefaultDatasourceResolver)
  }

  test("An exception is thrown if the given class does not exist") {
    intercept[ClassNotFoundException] {
      DatasourceResolver.resolverFor(sqlc).newInstanceOf("should.definitely.not.exist")
    }
  }
}
