package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.CaseSensitivityUtils._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.commands.{DescribeTableUsingCommand, UnresolvedDropCommand}
import org.apache.spark.sql.types.StructType

/**
  * Strategy for fixing and validating case sensitive plans.
  *
  * This strategy should handle plans that have content which are susceptible to case sensitivity
  * issues. This is done by converting [[String]]s or
  * [[org.apache.spark.sql.catalyst.TableIdentifier]]s to the correct case and by validating
  * schema information like [[StructType]]s.
  *
  * @param source The source from which a [[CaseSensitivityConverter]] can be inferred.
  * @tparam A The type of the source.
  */
case class FixCaseSensitivity[A: CaseSensitivitySource](source: A)
  extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case c@CreateTableUsing(tableIdent, schema, _, _, _, _, _) =>
      /** If a schema is set, it is validated to be correct within the current casing */
      c.copy(
        tableIdent = source.fixCase(tableIdent),
        userSpecifiedSchema = schema.map(schema => source.validatedSchema(schema).get))

    case c@CreateTablePartitionedByUsing(tableIdent, schema, _, pFun, pColumns, _, _, _, _) =>
      /** If a schema is set, it is validated to be correct within the current casing */
      c.copy(
        tableIdent = source.fixCase(tableIdent),
        userSpecifiedSchema = schema.map(schema => source.validatedSchema(schema).get),
        partitioningFunc = source.fixCase(pFun),
        partitioningColumns = pColumns.map(source.fixCase))

    case c: PartitioningFunctionCommand =>
      c.withName(source.fixCase(c.name))

    case d@DescribeTableUsingCommand(name, _, _) =>
      d.copy(source.fixCase(name))

    case r@RegisterTableCommand(tableName, _, _, _) =>
      r.copy(source.fixCase(tableName))

    case c: AbstractViewCommand =>
      c.withIdentifier(source.fixCase(c.identifier))

    case d@UnresolvedDropCommand(_, _, tableIdentifier, _) =>
      d.copy(tableIdentifier = source.fixCase(tableIdentifier))
  }
}
