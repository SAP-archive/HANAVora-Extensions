package org.apache.spark.unsafe.types.compat

private[spark] object CompatUTF8String {

  def fromString(s: String): UTF8String =
    org.apache.spark.unsafe.types.UTF8String.fromString(s)

  def fromBytes(bytes: Array[Byte]): UTF8String =
    org.apache.spark.unsafe.types.UTF8String.fromBytes(bytes)

  def concat(strings: UTF8String*): UTF8String =
    org.apache.spark.unsafe.types.UTF8String.concat(strings: _*)

}
