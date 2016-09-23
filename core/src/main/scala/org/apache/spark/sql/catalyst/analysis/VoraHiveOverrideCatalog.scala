package org.apache.spark.sql.catalyst.analysis

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import scala.collection.JavaConverters._

/**
  * The [[org.apache.spark.sql.hive.HiveContext]] contains a catalog refering to the Hive Metastore.
  * However, this catalog cannot store tables created for pure Spark. In order to do so we create
  * an trait that is mixed over the HiveMetastoreCatalog that stores Spark tables.
  *
  * This trait is a copy of the [[OverrideCatalog]] with the following changes:
  *  - The getTables method does not call the super method and thus does _not_ return any
  *     Hive tables (if present)
  *  - The tableExists method only looks for local tables
  *  - The lookup relation throws a NoSuchTableException if the table is not available locally
  */
trait VoraHiveOverrideCatalog extends Catalog {
  private[this] val overrides = new ConcurrentHashMap[String, LogicalPlan]

  private def getOverriddenTable(tableIdent: TableIdentifier): Option[LogicalPlan] = {
    if (tableIdent.database.isDefined) {
      None
    } else {
      Option(overrides.get(getTableName(tableIdent)))
    }
  }

  /**
    * Determines whether a table is defined in the catalog. The super class is _not_ called,
    * only local tables are considered.
    *
    * @param tableIdent
    * @return
    */
  abstract override def tableExists(tableIdent: TableIdentifier): Boolean = {
    getOverriddenTable(tableIdent).isDefined
  }

  /**
    * Looks up `tableIdent` in the local catalog
    *
    * @param tableIdent
    * @param alias
    * @return logical plan representing the table
    */
  abstract override def lookupRelation(
                                        tableIdent: TableIdentifier,
                                        alias: Option[String] = None): LogicalPlan = {
    getOverriddenTable(tableIdent) match {
      case Some(table) =>
        val tableName = getTableName(tableIdent)
        val tableWithQualifiers = Subquery(tableName, table)

        // If an alias was specified by the lookup, wrap the plan in a sub-query so that attributes
        // are properly qualified with this alias.
        alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)

      case None => throw new NoSuchTableException
    }
  }

  abstract override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    overrides.keySet().asScala.map(_ -> true).toSeq
  }

  override def registerTable(tableIdent: TableIdentifier, plan: LogicalPlan): Unit = {
    overrides.put(getTableName(tableIdent), plan)
  }

  override def unregisterTable(tableIdent: TableIdentifier): Unit = {
    if (tableIdent.database.isEmpty) {
      overrides.remove(getTableName(tableIdent))
    }
  }

  override def unregisterAllTables(): Unit = {
    overrides.clear()
  }
}
