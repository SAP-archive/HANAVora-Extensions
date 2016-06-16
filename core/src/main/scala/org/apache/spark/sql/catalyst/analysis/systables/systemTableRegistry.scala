package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.analysis.ResolveSystemTables
import org.apache.spark.sql.catalyst.plans.logical.{UnresolvedProviderBoundSystemTable, UnresolvedSparkLocalSystemTable, UnresolvedSystemTable}

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

/**
  * A registry for system tables.
  */
trait SystemTableRegistry {
  /**
    * Registers the system table class with the given name.
    *
    * @param name The name under which the class should be registered.
    * @param provider The [[SystemTableProvider]]
    */
  protected def register(name: String, provider: SystemTableProvider): Unit

  /**
    * Searches for the given name and returns an [[Option]][[SystemTable]]
    *
    * @param name The name to look up.
    * @return `None` if there is no [[SystemTableProvider]] registered for that name,
    *        [[Some]] otherwise.
    */
  protected def lookup(name: String): Option[SystemTableProvider]

  /**
    * Looks up the given [[UnresolvedSystemTable]] and creates the corresponding [[SystemTable]]
    *
    * If there is no corresponding [[SystemTable]], a [[SystemTableException.NotFoundException]]
    * is thrown.
    *
    * If the provider for the targeted [[SystemTable]] does not support the given configuration
    * (local or provider bound), a [[SystemTableException.InvalidProviderException]] is thrown.
    *
    * @param table The [[UnresolvedSystemTable]] to resolve.
    * @return A resolved [[SystemTable]]
    */
  def resolve(table: UnresolvedSystemTable): SystemTable = (table, lookup(table.name)) match {
    case (_: UnresolvedSparkLocalSystemTable,
          Some(p: SystemTableProvider with LocalSpark)) =>
      p.create()
    case (u: UnresolvedProviderBoundSystemTable,
          Some(p: SystemTableProvider with ProviderBound)) =>
      p.create(u.provider, u.options)
    case (u: UnresolvedSystemTable, Some(provider)) =>
      throw new SystemTableException.InvalidProviderException(provider, u)
    case (_, None) =>
      throw new SystemTableException.NotFoundException(table.name)
  }
}


/**
  * An implementation of the [[SystemTableRegistry]] trait.
  */
class SimpleSystemTableRegistry extends SystemTableRegistry {
  private val tables = new mutable.HashMap[String, SystemTableProvider]

  /**
    * Registers the system table class with the given name case insensitive.
    *
    * If a system table with the specified name is already registered,
    * it is overridden.
    *
    * @param name The name under which the class should be registered.
    * @param provider The [[SystemTableProvider]]
    */
  override def register(name: String, provider: SystemTableProvider): Unit = {
    tables(name.toLowerCase()) = provider
  }

  override def lookup(name: String): Option[SystemTableProvider] =
    tables.get(name.toLowerCase())
}


/**
  * The global [[SystemTableRegistry]] used by [[ResolveSystemTables]].
  */
object SystemTableRegistry extends SimpleSystemTableRegistry {
  register("tables", TablesSystemTableProvider)
  register("object_dependencies", DependenciesSystemTableProvider)
  register("table_metadata", MetadataSystemTableProvider)
  register("schemas", SchemaSystemTableProvider)
}
