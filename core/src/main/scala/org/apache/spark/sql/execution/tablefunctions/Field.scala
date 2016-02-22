package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{Metadata, StructField, DataType}

/** A structure to reflect table data upon. */
case class Field(
    tableName: String,
    name: String,
    dataType: DataType,
    isNullable: Boolean,
    metadata: Metadata)

object Field {
  def from(tableName: String, exp: NamedExpression): Field =
    Field(tableName, exp.name, exp.dataType, exp.nullable, exp.metadata)
}
