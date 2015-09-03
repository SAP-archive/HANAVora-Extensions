package org.apache.spark.sql.sources

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{CreateTableUsingTemporaryAwareCommand, ExecutedCommand, SparkPlan}

/**
 * Additional strategy to catch  persistent tables for datasources
 */
private[sql] object CreatePersistentTableStrategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    // Instantiates a create table command taking the temporary flag into account
    case CreateTableUsing(tableName,userSpecifiedSchema,provider,temporary,options,_,_) => {
      // here we are only handling cases where the ds actually supports temporary/persistent
      // tables
      SAPResolvedDataSource.lookupDataSource(provider).newInstance() match {
        case _ : TemporaryAndPersistentRelationProvider =>
          ExecutedCommand(CreateTableUsingTemporaryAwareCommand(tableName,
                                                          userSpecifiedSchema,
                                                          Array.empty[String],
                                                          provider,
                                                          options,
                                                          temporary)) :: Nil
        case _ => Nil
      }
    }
    case _ => Nil
  }
}
