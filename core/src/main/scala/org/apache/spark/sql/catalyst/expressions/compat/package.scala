package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.types.DataType

package object compat {
  implicit def dataType2AbstractDataType(dt: DataType): AbstractDataType =
    ConcreteDataType(dt)
}
