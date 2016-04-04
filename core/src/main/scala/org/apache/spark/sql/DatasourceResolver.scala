package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.ResolvedDataSource

/**
  * Looks up classes and creates instances of datasources.
  */
trait DatasourceResolver {
  def lookup[A](provider: String): Class[A]

  def newInstanceOf[A](provider: String): A = lookup[A](provider).newInstance()
}

/**
  * Looks up datasources by using [[ResolvedDataSource.lookupDataSource]]
  */
object DefaultDatasourceResolver extends DatasourceResolver {
  def lookup[A](provider: String): Class[A] = {
    val clazz = ResolvedDataSource.lookupDataSource(provider)
    clazz.asInstanceOf[Class[A]]
  }
}
