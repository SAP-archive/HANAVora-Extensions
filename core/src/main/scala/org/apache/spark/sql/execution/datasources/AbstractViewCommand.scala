package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.sources.{AbstractViewProvider, ViewKind}
import org.apache.spark.sql.{DatasourceResolver, SQLContext}

/**
  * A command to create a view.
  */
trait AbstractViewCommand extends RunnableCommand {
  /** The kind of the view. */
  val kind: ViewKind
  /** The identifier of the view. */
  val identifier: TableIdentifier

  /**
    * Returns a copy of this command with the identifier set to the given one.
    *
    * @param tableIdentifier The new [[TableIdentifier]].
    * @return A copy of this command with the new [[TableIdentifier]].
    */
  def withIdentifier(tableIdentifier: TableIdentifier): AbstractViewCommand
}

/**
  * An [[AbstractViewCommand]] whose execution relies on a provider.
  */
trait ProviderBound {
  self: AbstractViewCommand =>

  /** The package where the provider lies in. */
  val provider: String
  /** The options for this command. */
  val options: Map[String, String]

  /**
    * Executes the given operation with a valid provider.
    * Throws a [[ProviderException]] if a valid provider cannot be instantiated.
    * @param sqlContext The Spark [[SQLContext]]
    * @param b The operation to execute if a valid provider can be instantiated.
    * @tparam B The result type of the operation to execute.
    * @return The result of the operation.
    */
  def withValidProvider[B](sqlContext: SQLContext)(b: AbstractViewProvider[_] => B): B = {
    val resolver = DatasourceResolver.resolverFor(sqlContext)
    AbstractViewProvider.matcherFor(kind)(resolver.newInstanceOf(provider)) match {
      case Some(viewProvider) =>
        b(viewProvider)
      case _ =>
        throw new ProviderException(provider, "Does not support the " +
          s"execution of ${this.getClass.getSimpleName}")
    }
  }
}

class ProviderException(val provider: String, val reason: String)
  extends Exception(s"Exception using provider $provider: $reason")
