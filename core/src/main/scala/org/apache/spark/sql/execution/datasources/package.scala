package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier

package object datasources {

  /**
   * Applies the case-sensitivity settings from the provided [[SQLContext]] to
   * the given [[TableIdentifier]].
   *
   * @param sqlContext The [[SQLContext]] which settings are to be applied.
   * @param tableId The [[TableIdentifier]] to alter.
   * @return The altered [[TableIdentifier]].
   */
  def alterByCatalystSettings(sqlContext: SQLContext, tableId: TableIdentifier): TableIdentifier =
    if (sqlContext.catalog.conf.caseSensitiveAnalysis) {
      tableId
    } else {
      TableIdentifier(tableId.table.toLowerCase, tableId.database.map(_.toLowerCase))
    }

  /**
   * Applies the case-sensitivity settings from the provided [[SQLContext]] to
   * the given string identifier.
   *
   * @param sqlContext The [[SQLContext]] which settings are to be applied.
   * @param id The identifier to alter.
   * @return The altered identifier.
   */
  def alterByCatalystSettings(sqlContext: SQLContext, id: String): String =
    if (sqlContext.catalog.conf.caseSensitiveAnalysis) {
      id
    } else {
      id.toLowerCase
    }

}
