package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.Attribute

/** Enforces the given schema on sequences of values. */
case class SchemaEnforcer(schema: Seq[Attribute]) {
  // Caching of sequence size
  private lazy val schemaSize = schema.size

  /** Enforces the schema on the given sequence of tuples.
    *
    * Enforces the schema on the given sequence of
    * [[org.apache.spark.sql.catalyst.expressions.Attribute]]s
    * and their associated values.
    *
    * @param rows The [[Attribute]]s and their values
    * @return A sequence of enforced values
    * @throws AssertionError if the values could not fulfill the schema
    */
  def enforce(rows: Seq[Seq[Any]]): Seq[Seq[Any]] = {
    rows.map { row =>
      schema.zip(row.ensuring(_.size == schemaSize, "Schema and values size not equal")).map {
        case (attribute, value) =>
          new AttributeEnforcer(attribute).enforce(value)
      }
    }
  }
}
