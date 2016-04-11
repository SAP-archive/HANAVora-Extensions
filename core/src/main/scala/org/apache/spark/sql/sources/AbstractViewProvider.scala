package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, Persisted}

import scala.reflect._

trait AbstractViewProvider[A <: AbstractView with Persisted] {
  val tag: ClassTag[A]

  def create(createViewInput: CreateViewInput[A]): Unit

  def drop(dropViewInput: DropViewInput): Unit
}

abstract class BaseAbstractViewProvider[A <: AbstractView with Persisted: ClassTag]
  extends AbstractViewProvider[A] {
  val tag = implicitly[ClassTag[A]]
}

object AbstractViewProvider {
  def unapply[A <: AbstractView with Persisted: ClassTag]
      (any: Any): Option[AbstractViewProvider[A]] = {
    val multiProvider = MultiAbstractViewProvider.matcherFor[A]
    any match {
      case provider: AbstractViewProvider[_] if tagMatches(provider.tag) =>
        Some(provider.asInstanceOf[AbstractViewProvider[A]])
      case multiProvider(provider) =>
        Some(provider)
      case _ => None
    }
  }

  private def tagMatches[A: ClassTag](tag: ClassTag[_]): Boolean = {
    classTag[A].runtimeClass.isAssignableFrom(tag.runtimeClass)
  }
}

case class CreateViewInput[A <: AbstractView with Persisted](
    sqlContext: SQLContext,
    options: Map[String, String],
    identifier: TableIdentifier,
    view: A,
    allowExisting: Boolean,
    metadataViewSQL: String = "VIEW_SQL")

case class DropViewInput(
    sqlContext: SQLContext,
    options: Map[String, String],
    identifier: TableIdentifier,
    allowNotExisting: Boolean)
