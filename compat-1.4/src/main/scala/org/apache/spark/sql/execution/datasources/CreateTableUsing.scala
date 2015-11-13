package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.compat._

private[sql] object CreateTableUsing {

  private type ReturnType =
  (TableIdentifier, Option[StructType], String, Boolean, Map[String, String], Boolean, Boolean)

  def unapply(any: Any): Option[ReturnType] = any match {
    case org.apache.spark.sql.sources.CreateTableUsing(
    tableName,
    userSpecifiedSchema,
    provider,
    temporary,
    options,
    allowExisting,
    managedIfNoPath
    ) =>
      Some((
        TableIdentifier(tableName),
        userSpecifiedSchema,
        provider,
        temporary,
        options,
        allowExisting,
        managedIfNoPath))
    case _ => None
  }

}
