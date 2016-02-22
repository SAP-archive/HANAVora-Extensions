package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, ExprId}
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class LogicalPlanExtractorSuite extends FunSuite {
  def attr(name: String, dataType: DataType, id: Int, nullable: Boolean = false): Attribute = {
    AttributeReference(name, dataType, nullable)(ExprId(id))
  }

  val attributes = Seq(attr("foo", IntegerType, 0), attr("bar", StringType, 1))

  test("tablePart") {
    val project = Project(attributes, null)
    val tablePart = new LogicalPlanExtractor(project).tablePart
    assert(tablePart ==  "" :: Nil)
  }
}
