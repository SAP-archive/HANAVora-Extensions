package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{DataType, Metadata, StructField}

/**
  * A trait for objects that behave like fields.
  * @tparam A The class of the implementor.
  */
sealed trait FieldLike[A] {
  def name(field: A): String
  def dataType(field: A): DataType
  def nullable(field: A): Boolean
  def metadata(field: A): Metadata

  final def toAttribute(field: A): Attribute = {
    AttributeReference(name(field), dataType(field), nullable(field), metadata(field))()
  }
}

object FieldLike {
  /**
    * The implementation of the [[FieldLike]] type class for [[StructField]]s.
    */
  implicit object StructFieldLike extends FieldLike[StructField] {
    override def name(field: StructField): String = field.name
    override def metadata(field: StructField): Metadata = field.metadata
    override def nullable(field: StructField): Boolean = field.nullable
    override def dataType(field: StructField): DataType = field.dataType
  }

  /**
    * The implementation of the [[FieldLike]] for [[Attribute]]s.
    */
  implicit object AttributeFieldLike extends FieldLike[Attribute] {
    override def name(field: Attribute): String = field.name
    override def metadata(field: Attribute): Metadata = field.metadata
    override def nullable(field: Attribute): Boolean = field.nullable
    override def dataType(field: Attribute): DataType = field.dataType
  }
}
