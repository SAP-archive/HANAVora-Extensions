package org.apache.spark.sql.sources.sql

sealed trait ViewKind

object Plain extends ViewKind

object Dimension extends ViewKind

object Cube extends ViewKind

object ViewKind {
  def unapply(string: Option[String]): Option[ViewKind] = string match {
    case None => Some(Plain)
    case Some(str) => str match {
      case "dimension" => Some(Dimension)
      case "cube" => Some(Cube)
      case _ => None
    }
  }
}
