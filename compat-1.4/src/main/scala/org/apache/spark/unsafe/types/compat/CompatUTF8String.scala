package org.apache.spark.unsafe.types.compat

import org.apache.commons.lang3.StringUtils

/**
  * Provides backported methods from recent versions of
  * [[UTF8String]].
  *
  * DO NOT USE THIS DIRECTLY. Import
  * org.apache.spark.sql.catalyst.expressions.compat._
  * to use implicit conversions.
  *
  * @param us The underlying [[UTF8String]].
  */
private[spark] case class CompatUTF8String(us: UTF8String) {

  def numBytes: Int = us.getBytes.length

  private def getByte(i: Integer): java.lang.Byte =
    us.getBytes(i)

  private def copyUTF8String(start: Int, end: Int): UTF8String = {
    val len = end - start + 1
    val dstArray = new Array[java.lang.Byte](len)
    Array.copy(us.getBytes, start, dstArray, 0, len)
    UTF8String.fromBytes(dstArray.map(_.byteValue()))
  }

  def indexOf(v: UTF8String, start: Integer): Integer = {
    val str = us.toString()
    val other = v.toString()
    str.indexOf(other, start)
  }

  def trim(): UTF8String = {
    val str = us.toString()
    UTF8String.fromString(str.trim)
  }

  def trimLeft(): UTF8String = {
    var s = 0
    // skip all of the space (0x20) in the left side
    while (s < this.numBytes && getByte(s) <= 0x20 && getByte(s) >= 0x00) s += 1
    if (s == this.numBytes) {
      // empty string
      UTF8String.fromBytes(Array())
    } else {
      copyUTF8String(s, this.numBytes - 1)
    }
  }

  def trimRight(): UTF8String = {
    var e = numBytes - 1
    // skip all of the space (0x20) in the right side
    while (e >= 0 && getByte(e) <= 0x20 && getByte(e) >= 0x00) e -= 1

    if (e < 0) {
      // empty string
      UTF8String.fromBytes(Array())
    } else {
      copyUTF8String(0, e)
    }
  }

  def lpad(len: Int, pad: UTF8String): UTF8String = {
    val strPad = pad.toString
    val str = us.toString()
    UTF8String.fromString(StringUtils.substring(StringUtils.leftPad(str, len, strPad), 0, len))
  }

  def rpad(len: Int, pad: UTF8String): UTF8String = {
    val strPad = pad.toString
    val str = us.toString()
    UTF8String.fromString(StringUtils.substring(StringUtils.rightPad(str, len, strPad), 0, len))
  }

  def reverse(): UTF8String = {
    val str = us.toString()
    UTF8String.fromString(StringUtils.reverse(str))
  }

  def numChars(): Int = {
    us.toString().codePointCount(0, us.length())
  }

}

private[spark] object CompatUTF8String {

  def fromString(s: String): UTF8String =
    org.apache.spark.sql.types.UTF8String(s)

  def fromBytes(bytes: Array[Byte]): UTF8String =
    org.apache.spark.sql.types.UTF8String(bytes)

  def concat(strings: UTF8String*): UTF8String = {
    val sb = new StringBuilder
    strings foreach { s =>
      if (s == null) {
        return null // scalastyle:ignore
      }
      sb.append(s.toString())
    }
    UTF8String.fromString(sb.toString())
  }

}
