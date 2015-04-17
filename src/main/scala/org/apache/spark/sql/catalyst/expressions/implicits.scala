package org.apache.spark.sql.catalyst.expressions

object implicits {

  implicit class ExpressionsOps(expression: Expression) {
    def extractAttributes: Seq[AttributeReference] = ExpressionsOps.extract(expression)
  }

  private object ExpressionsOps {
    private def extract(expression: Expression): Seq[AttributeReference] =
      expression match {
        case attr@AttributeReference(_, _, _, _) => Seq(attr)
        case _ => expression.children.flatMap(extract)
      }
  }

}
