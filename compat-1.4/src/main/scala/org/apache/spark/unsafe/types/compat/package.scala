package org.apache.spark.unsafe.types

/**
  * Provides compatibility shims for [[org.apache.spark.unsafe.types]].
  *
  * {{{
  *   // Compatibility import.
  *   import org.apache.spark.unsafe.types.compat._
  * }}}
  *
  * Classes inside this package should NOT be imported directly.
  */
package object compat {

  type UTF8String = org.apache.spark.sql.types.UTF8String
  val UTF8String = CompatUTF8String

  implicit def utf8String2CompatUtf8String(s: UTF8String): CompatUTF8String =
    CompatUTF8String(s)

}
