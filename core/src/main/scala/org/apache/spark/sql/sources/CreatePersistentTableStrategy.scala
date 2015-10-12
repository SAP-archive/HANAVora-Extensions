package org.apache.spark.sql.sources

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CreateTableUsingTemporaryAwareCommand, ExecutedCommand, SparkPlan}

/**
 * Additional strategy to catch  persistent tables for datasources
 */
private[sql] object CreatePersistentTableStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // Currently we only handle cases where the user wants to instantiate a
    // persistent relation any other cases has to be handled by the datasource itself
    case CreateTableUsing(tableName,
        userSpecifiedSchema, provider, false, options, allowExisting, _) =>
      ResolvedDataSource.lookupDataSource(provider).newInstance() match {
        case _: TemporaryAndPersistentNature =>
          ExecutedCommand(CreateTableUsingTemporaryAwareCommand(tableName,
            userSpecifiedSchema,
            Array.empty[String],
            provider,
            options,
            isTemporary = false,
            allowExisting)) :: Nil
        case _ => Nil
      }

    case _ => Nil
  }
}
