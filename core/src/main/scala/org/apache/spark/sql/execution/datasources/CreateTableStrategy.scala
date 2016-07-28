package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{DatasourceResolver, SQLContext, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, SparkPlan}
import org.apache.spark.sql.sources.TemporaryAndPersistentNature

/**
 * Strategy for table creation of datasources with [[TemporaryAndPersistentNature]].
 */
private[sql] case class CreateTableStrategy(sqlContext: SQLContext) extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // Currently we only handle cases where the user wants to instantiate a
    // persistent relation any other cases has to be handled by the datasource itself
    case CreateTableUsing(tableName,
        userSpecifiedSchema, provider, temporary, options, allowExisting, _) =>
      DatasourceResolver.resolverFor(sqlContext).newInstanceOf(provider) match {
        case _: TemporaryAndPersistentNature =>
          ExecutedCommand(CreateTableUsingTemporaryAwareCommand(tableName,
            userSpecifiedSchema,
            Array.empty[String],
            None,
            None,
            provider,
            options,
            temporary,
            allowExisting)) :: Nil
        case _ => Nil
      }

    case CreateTablePartitionedByUsing(tableId, userSpecifiedSchema, provider,
    partitioningFunction, partitioningColumns, temporary, options, allowExisting, _) =>
      ResolvedDataSource.lookupDataSource(provider).newInstance() match {
        case _: TemporaryAndPersistentNature =>
          ExecutedCommand(CreateTableUsingTemporaryAwareCommand(
            tableId,
            userSpecifiedSchema,
            Array.empty[String],
            Some(partitioningFunction),
            Some(partitioningColumns),
            provider,
            options,
            isTemporary = false,
            allowExisting)) :: Nil
        case _ => Nil
      }
    case _ => Nil
  }
}
