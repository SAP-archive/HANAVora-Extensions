package org.apache.spark.sql

import org.apache.spark.sql.execution.datasources.ResolvedDataSource

import scala.reflect.ClassTag

/**
  * Looks up classes and creates instances of datasources.
  */
trait DatasourceResolver {
  def lookup[A](provider: String): Class[A]

  def newInstanceOf[A](provider: String): A = lookup[A](provider).newInstance()

  final def create[A: ClassTag](provider: String): A = {
    val instance = lookup[A](provider).newInstance()
    val tag = implicitly[ClassTag[A]]
    if (tag.runtimeClass.isInstance(instance)) {
      instance
    } else {
      throw new RuntimeException(s"Created instance $instance not of expected type " +
        s"${tag.runtimeClass.getSimpleName}")
    }
  }
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
