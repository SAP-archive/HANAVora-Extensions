package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, LogicalPlan, Persisted}
import org.apache.spark.sql.sources.sql.ViewKind

import scala.reflect._

trait AbstractViewProvider[A <: AbstractView with Persisted] {
  val tag: ClassTag[A]

  def create(createViewInput: CreateViewInput): ViewHandle

  def drop(dropViewInput: DropViewInput): Unit
}

trait ViewHandle {
  def drop(): Unit
}

abstract class BaseAbstractViewProvider[A <: AbstractView with Persisted: ClassTag]
  extends AbstractViewProvider[A] {
  val tag = implicitly[ClassTag[A]]
}

object AbstractViewProvider {
  def matcherFor(kind: ViewKind)(any: Any): Option[AbstractViewProvider[_]] = {
    val multiProvider = MultiAbstractViewProvider.matcherFor(kind)
    any match {
      case provider: AbstractViewProvider[_] if tagMatches(provider.tag) =>
        Some(provider)
      case multiProvider(provider) =>
        Some(provider)
      case _ => None
    }
  }

  private def tagMatches[A: ClassTag](tag: ClassTag[_]): Boolean = {
    classTag[A].runtimeClass.isAssignableFrom(tag.runtimeClass)
  }
}

case class CreateViewInput(
    sqlContext: SQLContext,
    plan: LogicalPlan,
    viewSql: String,
    options: Map[String, String],
    identifier: TableIdentifier,
    allowExisting: Boolean)

case class DropViewInput(
    sqlContext: SQLContext,
    options: Map[String, String],
    identifier: TableIdentifier,
    allowNotExisting: Boolean)
