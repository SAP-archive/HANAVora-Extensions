package org.apache.spark.sql.catalyst.expressions

object BinarySymbolExpression {

  def isBinaryExpressionWithSymbol(be: BinaryExpression): Boolean =
    org.apache.spark.SPARK_VERSION match {
      case x if x startsWith "1.4" =>
        true
      case x if x startsWith "1.5" =>
        be.isInstanceOf[BinaryArithmetic] || be.isInstanceOf[BinaryComparison]
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }

  def getBinaryExpressionSymbol(be: BinaryExpression): String =
    org.apache.spark.SPARK_VERSION match {
      case x if x startsWith "1.4" =>
        be.getClass.getDeclaredMethod("symbol").invoke(be).asInstanceOf[String]
      case x if x startsWith "1.5" =>
        be match {
          case be: BinaryComparison => be.symbol
          case be: BinaryArithmetic => be.symbol
          case _ => sys.error(s"${be.getClass.getName} has no symbol attribute")
        }
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }

  def unapply(any: Any): Option[(Expression, String, Expression)] = any match {
    case be: BinaryExpression if isBinaryExpressionWithSymbol(be) =>
      Some((be.left, getBinaryExpressionSymbol(be), be.right))
    case _ => None
  }

}
