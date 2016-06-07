package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.Catalog

package object datasources {

  /**
   * Applies the case-sensitivity settings from the provided [[Catalog]] configuration to
   * the given [[TableIdentifier]].
   *
   * @param catalog The [[Catalog]] implementation which settings are to be applied.
   * @param tableId The [[TableIdentifier]] to alter.
   * @return The altered [[TableIdentifier]].
   */
  def alterByCatalystSettings(catalog: Catalog, tableId: TableIdentifier): TableIdentifier =
    if (catalog.conf.caseSensitiveAnalysis) {
      tableId
    } else {
      TableIdentifier(tableId.table.toLowerCase, tableId.database.map(_.toLowerCase))
    }

  /**
   * Applies the case-sensitivity settings from the provided [[Catalog]] configuration to
   * the given string identifier.
   *
   * @param catalog The [[Catalog]] implementation which settings are to be applied.
   * @param id The identifier to alter.
   * @return The altered identifier.
   */
  def alterByCatalystSettings(catalog: Catalog, id: String): String =
    if (catalog.conf.caseSensitiveAnalysis) {
      id
    } else {
      id.toLowerCase
    }

}
