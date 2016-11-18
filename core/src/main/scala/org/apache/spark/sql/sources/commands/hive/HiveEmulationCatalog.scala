package org.apache.spark.sql.sources.commands.hive

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Catalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * A stackable trait that intercepts catalog calls with table identifiers
  * and redirects them to an implementor.
  */
trait HiveEmulationCatalog extends Catalog {

  /** @inheritdoc */
  abstract override protected def getTableName(tableIdent: TableIdentifier): String =
    super.getTableName(anonymizeIfNecessary(tableIdent))

  /** @inheritdoc */
  abstract override def tableExists(tableIdent: TableIdentifier): Boolean =
    super.tableExists(anonymizeIfNecessary(tableIdent))

  /** @inheritdoc */
  abstract override def lookupRelation(tableIdent: TableIdentifier,
                                       alias: Option[String]): LogicalPlan =
    super.lookupRelation(anonymizeIfNecessary(tableIdent), alias)

  /** @inheritdoc */
  abstract override def refreshTable(tableIdent: TableIdentifier): Unit =
    super.refreshTable(anonymizeIfNecessary(tableIdent))

  /** @inheritdoc */
  abstract override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit =
    super.registerTable(anonymizeIfNecessary(tableIdent), plan)

  /** @inheritdoc */
  abstract override def unregisterTable(tableIdent: TableIdentifier): Unit =
    super.unregisterTable(anonymizeIfNecessary(tableIdent))

  /** @inheritdoc */
  private def anonymizeIfNecessary(tableIdentifier: TableIdentifier): TableIdentifier =
    if (hiveEmulationEnabled) tableIdentifier.copy(database = None) else tableIdentifier

  /** `true` if hive emulation is enabled, `false` otherwise. */
  def hiveEmulationEnabled: Boolean
}
