package org.apache.spark.util

import org.scalatest.FunSuite
import org.apache.spark.sql.util.GenericUtil._

class GenericUtilSuite extends FunSuite {
  test("matchOptional None") {
    val res = 1 matchOptional {
      case 2 => 3
    }
    assert(res.isEmpty)
  }

  test("matchOptional Some") {
    val res = 1 matchOptional {
      case 1 => 2
    }
    assert(res == Some(2))
  }
}
