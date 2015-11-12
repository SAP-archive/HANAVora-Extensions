package org.apache.spark.unsafe.types

package object compat {

  type UTF8String = org.apache.spark.sql.types.UTF8String
  val UTF8String = CompatUTF8String

  implicit def utf8String2CompatUtf8String(s: UTF8String): CompatUTF8String =
    CompatUTF8String(s)

}
