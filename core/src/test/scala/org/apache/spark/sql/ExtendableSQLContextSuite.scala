package org.apache.spark.sql

import org.apache.spark.MockitoSparkContext
import org.scalatest.FunSuite

class ExtendableSQLContextSuite extends FunSuite with MockitoSparkContext {

  test("instantiate ExtendableSQLContext") {
    val sqlc = new ExtendableSQLContext(sc)
    sqlc.analyzer
    sqlc
  }

}
