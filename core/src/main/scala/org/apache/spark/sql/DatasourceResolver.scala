package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.ResolvedDataSource

import scala.collection.parallel.mutable
import scala.reflect.ClassTag

/**
  * Looks up classes and creates instances of datasources.
  */
trait DatasourceResolver {
  def lookup(provider: String): Class[_]

  def newInstanceOf(provider: String): Any = lookup(provider).newInstance()

  final def newInstanceOfTyped[A: ClassTag](provider: String): A = {
    val instance = newInstanceOf(provider)
    val tag = implicitly[ClassTag[A]]
    if (tag.runtimeClass.isInstance(instance)) {
      instance.asInstanceOf[A]
    } else {
      throw new RuntimeException(s"Created instance $instance not of expected type " +
        s"${tag.runtimeClass.getSimpleName}")
    }
  }
}

/**
  * A registry that stores associations between [[SQLContext]]s and [[DatasourceResolver]]s.
  */
class DatasourceRegistry {
  private val sqlContextMappings: mutable.ParHashMap[SQLContext, DatasourceResolver] =
    mutable.ParHashMap.empty[SQLContext, DatasourceResolver]

  /**
    * Returns a [[DatasourceResolver]] for the given Spark [[SQLContext]].
    *
    * If there is no [[DatasourceResolver]] associated to the given Spark [[SQLContext]],
    * the global [[DefaultDatasourceResolver]] is returned.
    *
    * @param sqlContext The Spark [[SQLContext]]
    * @return A [[DatasourceResolver]] that is associated to the given Spark [[SQLContext]] or
    *         the global [[DefaultDatasourceResolver]] if there is no association.
    */
  def resolverFor(sqlContext: SQLContext): DatasourceResolver =
    sqlContextMappings.getOrElse(sqlContext, DefaultDatasourceResolver)

  /**
    * Runs the code block with the [[DatasourceResolver]] associated to the Spark [[SQLContext]].
    *
    * @param sqlContext The Spark [[SQLContext]].
    * @param resolver The [[DatasourceResolver]] to associate with the Spark [[SQLContext]].
    * @param block The block to be executed within the association.
    * @tparam A The return type.
    * @return The result of the given block executed within the association.
    */
  def withResolver[A](sqlContext: SQLContext,
                      resolver: DatasourceResolver)(block: => A): A = {
    sqlContextMappings(sqlContext) = resolver
    try {
      block
    } finally {
      sqlContextMappings.remove(sqlContext)
    }
  }
}

object DatasourceResolver extends DatasourceRegistry

/**
  * Looks up datasources by using [[ResolvedDataSource.lookupDataSource]]
  */
class DefaultDatasourceResolver extends DatasourceResolver {
  def lookup(provider: String): Class[_] = ResolvedDataSource.lookupDataSource(provider)
}

object DefaultDatasourceResolver extends DefaultDatasourceResolver
