package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{AbstractView, Persisted}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.AbstractViewProvider
import org.apache.spark.sql.{DatasourceResolver, DefaultDatasourceResolver, Row, SQLContext}

import scala.reflect.ClassTag

/**
  * A command to create a view.
  * @tparam A The type of the view.
  */
trait AbstractViewCommand[A <: AbstractView] extends RunnableCommand {
  /** The identifier of the view. */
  val identifier: TableIdentifier
}

/**
  * An [[AbstractViewCommand]] whose execution relies on a provider.
  * @tparam A The type of the view the provider should be able to handle.
  */
trait ProviderBound[A <: AbstractView with Persisted] {
  self: AbstractViewCommand[A] =>

  /** The [[ClassTag]] of the targeted view class. */
  val tag: ClassTag[A]
  /** The package where the provider lies in. */
  val provider: String
  /** The options for this command. */
  val options: Map[String, String]

  override def run(sqlContext: SQLContext): Seq[Row] =
    execute(sqlContext)(DefaultDatasourceResolver)

  /**
    * Executes this command with the given sqlContext and resolver.
    * By default, the [[DefaultDatasourceResolver]] is used if none is implicitly
    * specified.
    * @param sqlContext The sqlContext to execute with.
    * @param resolver The resolver of the provider.
    * @return A sequence of rows.
    */
  def execute(sqlContext: SQLContext)(implicit resolver: DatasourceResolver): Seq[Row]

  /**
    * Executes the given operation with a valid provider.
    * Throws a [[ProviderException]] if a valid provider cannot be instantiated.
    * @param b The operation to execute if a valid provider can be instantiated.
    * @param resolver The resolver to get the provider.
    * @tparam B The result type of the operation to execute.
    * @return The result of the operation.
    */
  def withValidProvider[B](b: AbstractViewProvider[A] => B)
                          (implicit resolver: DatasourceResolver): B = {
    AbstractViewProvider.unapply[A](resolver.newInstanceOf(provider))(tag) match {
      case Some(viewProvider) =>
        b(viewProvider)
      case _ =>
        throw new ProviderException(provider, "Does not support the " +
          s"execution of ${this.getClass.getSimpleName}")
    }
  }
}

object ProviderBound {
  implicit val defaultProvider = DefaultDatasourceResolver
}

class ProviderException(val provider: String, val reason: String)
  extends Exception(s"Exception using provider $provider: $reason")

/**
  * A base class for a view command that has [[ClassTag]] information about the view type
  * it targets.
  * @tparam A The type of the view.
  */
abstract class TaggedViewCommand[A <: AbstractView: ClassTag] extends AbstractViewCommand[A] {
  val tag = implicitly[ClassTag[A]]
}
