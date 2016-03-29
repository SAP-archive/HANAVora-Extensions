package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
  * Describes the given structure.
  * @param structure The structure to describe.
  * @tparam A The type for which an implementation of [[FieldLike]] is implicitly available.
  */
case class StructureDescriber[A: FieldLike](structure: Seq[A]) extends Describable {
  override def describe(): Any = {
    structure.map { field =>
      val fieldLike = implicitly[FieldLike[A]]
      Row(fieldLike.name(field), fieldLike.dataType(field).typeName)
    }
  }

  override def describeOutput: DataType = StructureDescriber.fieldType
}

object StructureDescriber {
  val fieldType: ArrayType =
    ArrayType(
      StructType(
        StructField("name", StringType, nullable = false) ::
        StructField("dataType", StringType, nullable = false) :: Nil))
}
