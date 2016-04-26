package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.catalyst.analysis.ResolveSystemTables
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedSystemTable

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
    * @tparam C The class of the [[SystemTable]]
    */
  protected def register[C <: SystemTable: ClassTag](name: String): Unit

  /**
    * Searches for the given name and returns an [[Option]][[SystemTable]]
    *
    * @param name The name to look up.
    * @return `None` if there is no [[SystemTable]] corresponding to the name,
    *        [[Some]] otherwise.
    */
  protected def lookup(name: String): Option[Class[_ <: SystemTable]]

  /**
    * Looks up the given [[UnresolvedSystemTable]] and creates the corresponding [[SystemTable]]
    *
    * If there is no corresponding [[SystemTable]], a [[SystemTableException.NotFoundException]]
    * is thrown.
    *
    * @param table The [[UnresolvedSystemTable]] to resolve.
    * @return A resolved [[SystemTable]]
    */
  def resolve(table: UnresolvedSystemTable): SystemTable = lookup(table.name) match {
    case Some(clazz) =>
      instantiate(table, clazz)
    case None =>
      throw new SystemTableException.NotFoundException(table.name)
  }

  /**
    * Instantiates the system table.
    *
    * First checks for [[String]] [[Map]], then for [[String]], then for [[Map]]
    * and finally for no-arg constructors and creates an instance with the appropriate
    * arguments.
    *
    * @param table The [[UnresolvedSystemTable]]
    * @param clazz The class of which an instance shall be created
    * @return An instance of the given class.
    */
  private def instantiate(table: UnresolvedSystemTable, clazz: Class[_ <: SystemTable]) = {
    val stringClass = classOf[String]
    val mapClass = classOf[Map[_, _]]
    val provider = table.provider
    val options = table.options

    val constructorToArgumentBindings =
      Seq(
        Seq(stringClass, mapClass)    -> Seq(provider, options),
        Seq(stringClass)              -> Seq(provider),
        Seq(mapClass)                 -> Seq(options),
        Seq()                         -> Seq())

    constructorToArgumentBindings.view.map {
      case (constructorParams, args) =>
        (Try(clazz.getConstructor(constructorParams:_*)).toOption, args)
    }.collectFirst {
      case (Some(constructor), args) =>
        constructor.newInstance(args:_*)
    }.getOrElse(sys.error("No matching constructor found"))
  }
}


/**
  * An implementation of the [[SystemTableRegistry]] trait.
  */
class SimpleSystemTableRegistry extends SystemTableRegistry {
  private val tables = new mutable.HashMap[String, Class[_ <: SystemTable]]

  /**
    * Registers the system table class with the given name case insensitive.
    *
    * If a system table with the specified name is already registered,
    * it is overridden.
    *
    * @param name The name under which the class should be registered.
    * @tparam C The class of the [[SystemTable]]
    */
  override def register[C <: SystemTable: ClassTag](name: String): Unit = {
    tables(name.toLowerCase()) = implicitly[ClassTag[C]].runtimeClass.asInstanceOf[Class[C]]
  }

  override def lookup(name: String): Option[Class[_ <: SystemTable]] =
    tables.get(name.toLowerCase())
}


/**
  * The global [[SystemTableRegistry]] used by [[ResolveSystemTables]].
  */
object SystemTableRegistry extends SimpleSystemTableRegistry {
  register[TablesSystemTable]("tables")
}
