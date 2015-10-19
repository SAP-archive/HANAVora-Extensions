package org.apache.spark.sql.util

import scala.language.implicitConversions

/**
 * Class to implicitly add validation to obtain properties from any map.
 * @param m map to validate
 * @tparam K key type
 * @tparam V value type
 */
class ValidatingPropertyMap[K, V](val m: Map[K, V]) {
  def getString(key: K): String =
    m.get(key) match {
      case Some(value: String) => value
      case Some(value) => value.toString
      case None =>
        throw new RuntimeException(s"$key is mandatory")
    }

  def getBoolean(key: K, default: => Boolean): Boolean =
    m.get(key) match {
      case Some(value: Boolean) => value
      case Some(value) => value.toString.toBoolean
      case None => default
    }

  def getMandatoryBoolean(key: K): Boolean =
    m.get(key) match {
      case Some(value: Boolean) => value
      case Some(value) => value.toString.toBoolean
      case None => throw new RuntimeException(s"$key is mandatory")
    }

  def getString(key: K, default: => String): String = {
    m.get(key) match {
      case Some(value: String) => value
      case Some(value) => value.toString
      case None => default
    }
  }

  def getInt(key: K, default: => Int): Int = {
    m.get(key) match {
      case Some(value: String) => Integer.parseInt(value)
      case Some(value: Int) => value
      case None => default
      case _ => default
    }
  }

  def getSeq(key: K, default: => Seq[String]): Seq[String] =
    m.get(key) match {
      case Some(value: String) => value.split(",").map(_.trim).toSeq
      case Some(value) => value.toString.split(",").map(_.trim).toSeq
      case None => default
    }

  def getMandatorySeq(key: K): Seq[String] =
    this.getMandatory(key) match {
      case value: String => value.split(",").map(_.trim).toSeq
      case value => value.toString.split(",").map(_.trim).toSeq
    }

  def getMandatory(key: K): V =
    m.get(key) match {
      case Some(value) => value
      case None =>
        throw new RuntimeException(s"$key is mandatory")
    }


}

object ValidatingPropertyMap {
  implicit def map2ValidatingPropertyMap[K, V](m: Map[K, V]): ValidatingPropertyMap[K, V] =
    new ValidatingPropertyMap[K, V](m)
}
