package org.apache.spark.sql.sources.sql

import org.apache.spark.sql.catalyst.plans.logical._

import scala.reflect._

sealed trait ViewKind {
  type A <: AbstractView with Persisted
  val relatedTag: ClassTag[A]
}

object Plain extends ViewKind {
  type A = PersistedView
  val relatedTag = classTag[A]
}

object Dimension extends ViewKind {
  type A = PersistedDimensionView
  val relatedTag = classTag[A]
}

object Cube extends ViewKind {
  type A = PersistedCubeView
  val relatedTag = classTag[A]
}

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
