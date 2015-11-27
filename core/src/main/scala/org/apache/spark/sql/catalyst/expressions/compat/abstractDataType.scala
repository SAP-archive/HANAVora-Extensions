package org.apache.spark.sql.catalyst.expressions.compat

import org.apache.spark.sql.types.DataType

//
// Shim for AbstractDataType to Spark 1.4.
//

private[expressions] abstract class AbstractDataType {

  private[sql] def defaultConcreteType: DataType

}

private[expressions] case class TypeCollection(types: AbstractDataType*)
  extends AbstractDataType {

  require(types.nonEmpty, s"TypeCollection ($types) cannot be empty")

  override private[sql] def defaultConcreteType: DataType = types.head.defaultConcreteType

}

private[expressions] case class ConcreteDataType(override val defaultConcreteType: DataType)
  extends AbstractDataType
