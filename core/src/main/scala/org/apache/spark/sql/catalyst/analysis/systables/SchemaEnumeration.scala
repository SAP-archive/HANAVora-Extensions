package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.types.{DataType, Metadata, StructField, StructType}

import scala.collection.mutable

/**
  * An enumeration of [[StructField]]s that are part of a [[StructField]].
  */
class SchemaEnumeration {
  // scalastyle:off method.name
  /**
    * Adds a [[StructField]] with the given attributes to this enumeration.
    *
    * @param name The name of the [[StructField]].
    * @param dataType The [[DataType]] of the [[StructField]].
    * @param nullable `true` if this [[StructField]] should be nullable, `false` otherwise.
    * @param metadata The [[Metadata]] of this [[StructField]].
    * @return The [[StructField]] created with the given parameters. Also adds the field
    *         to this enumeration.
    * @throws IllegalStateException If the `schema` of this was already accessed.
    * @throws IllegalStateException If there is already a [[StructField]] with the given name.
    */
  protected final def Field(name: String,
                            dataType: DataType,
                            nullable: Boolean = true,
                            metadata: Metadata = Metadata.empty): StructField = {
    if (_schema.isDefined) {
      throw new IllegalStateException("Already built a schema with defined fields")
    }
    fieldMap.get(name) match {
      case Some(field) =>
        throw new IllegalStateException(s"Already defined a field with name '$name': $field")
      case None =>
        val field = StructField(name, dataType, nullable, metadata)
        fieldMap(name) = field
        field
    }
  }
  // scalastyle:on method.name

  private val fieldMap = new mutable.LinkedHashMap[String, StructField]()

  private var _schema: Option[StructType] = None

  /**
    * Returns the [[StructType]] schema of this enumeration.
    *
    * @return The [[StructType]] schema of this.
    */
  final def schema: StructType =
    _schema.getOrElse {
      val structType = StructType(fieldMap.values.toSeq)
      _schema = Some(structType)
      structType
    }
}
