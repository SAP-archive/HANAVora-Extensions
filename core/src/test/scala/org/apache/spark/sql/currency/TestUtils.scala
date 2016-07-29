package org.apache.spark.sql.currency

import org.scalatest.exceptions.TestFailedException

object TestUtils  {
  /**
    * Convenience function to compare a [[java.math.BigDecimal]] to either another
    * [[java.math.BigDecimal]] or to a [[String]] representation. The conversion uses
    * [[java.math.BigDecimal.compareTo]], i.e. it ignores the scale of the arguments (2.0 == 2.00).
    *
    * @param left
    * @param right
    * @throws TestFailedException if the two arguments did not compare equal
    */
  def assertComparesEqual(left: java.math.BigDecimal)(right: Any): Unit = {
    right match {
      case bd: java.math.BigDecimal => internalComp(left, bd)
      case s: String => internalComp(left, new java.math.BigDecimal(s))
      case _ => throw new TestFailedException(s"incompatible types: $left vs. $right", 1)
    }
  }

  private def internalComp(left: java.math.BigDecimal, right: java.math.BigDecimal) = {
    if (left.compareTo(right) != 0) {
      throw new TestFailedException(s"$left did not compare equal to $right", 2)
    }
  }
}
