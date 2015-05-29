package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.analysis.{SimpleCatalog, OverrideCatalog}
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}
import org.apache.spark.sql.hive._

import scala.collection.mutable

/*
 * HiveMetastoreCatalogProxy is needed as original HiveMetastoreCatalog
 * is not case sensitive and it does not throw the same type of exceptions as
 * SimpleCatalog throws. Methods are overridden with SimpleCatalog implementation
 *
 * TODO: Simplify this class
 *   Instantiate a SimpleCatalog and call methods on this instance
 *   instead of reimplementing its methods again
 */
class HiveMetastoreCatalogProxy(hive: HiveContext) extends HiveMetastoreCatalog(hive: HiveContext)
with OverrideCatalog
{

  override val caseSensitive = true


   val tables = new mutable.HashMap[String, LogicalPlan]()

  override def registerTable(
                              tableIdentifier: Seq[String],
                              plan: LogicalPlan): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables += ((getDbTableName(tableIdent), plan))
  }

  override def unregisterTable(tableIdentifier: Seq[String]) = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables -= getDbTableName(tableIdent)
  }

  override def unregisterAllTables() = {
    tables.clear()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    tables.get(getDbTableName(tableIdent)) match {
      case Some(_) => true
      case None => false
    }
  }

  override def lookupRelation(
                               tableIdentifier: Seq[String],
                               alias: Option[String] = None): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val tableFullName = getDbTableName(tableIdent)
    val table = tables.getOrElse(tableFullName, sys.error(s"Table Not Found: $tableFullName"))
    val tableWithQualifiers = Subquery(tableIdent.last, table)

    // If an alias was specified by the lookup, wrap the plan in a subquery so that attributes are
    // properly qualified with this alias.
    alias.map(a => Subquery(a, tableWithQualifiers)).getOrElse(tableWithQualifiers)
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    tables.map {
      case (name, _) => (name, true)
    }.toSeq
  }

  override def refreshTable(databaseName: String, tableName: String): Unit = {
    throw new UnsupportedOperationException
  }

  override protected def processTableIdentifier(tableIdentifier: Seq[String]): Seq[String] = {
    if (!caseSensitive) {
      tableIdentifier.map(_.toLowerCase)
    } else {
      tableIdentifier
    }
  }

  override protected def getDbTableName(tableIdent: Seq[String]): String = {
    val size = tableIdent.size
    if (size <= 2) {
      tableIdent.mkString(".")
    } else {
      tableIdent.slice(size - 2, size).mkString(".")
    }
  }

  override protected def getDBTable(tableIdent: Seq[String]) : (Option[String], String) = {
    (tableIdent.lift(tableIdent.size - 2), tableIdent.last)
  }

}

