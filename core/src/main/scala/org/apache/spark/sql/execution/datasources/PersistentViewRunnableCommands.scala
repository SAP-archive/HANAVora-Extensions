package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.ViewProvider
import org.apache.spark.sql.sources.sql.View
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{PersistedView, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import SqlContextAccessor._

/**
  * Pushes down a view creation and persistence to the given data source.
  *
  * Note that the view will also be registered as usual in the Spark catalog as well.
  *
  * Also note that even if the view statement contains relations that do not exist in the
  * data source catalog we can still store it there. (in case of Vora for example, the view might
  * reference a table in a HANA system which is ok). So when the view is fetched from the data
  * source back in Spark and if the view contains references to relations that do not exist anymore
  * then the execution of the view will fail.
  *
  * @param viewIdentifier The view identifier
  * @param provider The name of the datasource that should provide support for
  *                 persisted views.
  * @param options The view options.
  * @param allowExisting Determine what to do if a view with the same name already exists
  *                      in the data source catalog. It set to true nothing will happen
  *                      however if it is set to false then an exception will be thrown.
  */
private[sql]
case class CreatePersistentViewRunnableCommand(viewIdentifier: TableIdentifier,
                                               plan: LogicalPlan,
                                               provider: String,
                                               options: Map[String, String],
                                               allowExisting: Boolean)
  extends RunnableCommand {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = plan match {

    case PersistedView(child) =>
      val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()
      dataSource match {

        case viewProvider: ViewProvider =>
          viewProvider.createView(sqlContext, View(viewIdentifier, plan),
            new CaseInsensitiveMap(options), allowExisting)

          // use catalog tableExists method as it takes into account whether Spark SQL
          // is case-sensitive or not
          if (sqlContext.catalog.tableExists(viewIdentifier.toSeq)) {

            if (!allowExisting) {
              sys.error(s"Relation ${viewIdentifier.toSeq} already exists")
            }

          }

          sqlContext.registerRawPlan(child, viewIdentifier.table)
          Seq.empty

        case _ => throw new RuntimeException(s"The provided data source $provider " +
          s"does not support creating and persisting views.")
      }

    case _ => throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
      s"expects a ${PersistedView.getClass.getSimpleName} plan")
  }
}

/**
  * Pushes down a drop view command to the given data source. Note that we definitely need the
  * provider in this command because the view itself is not a relation that can be queried (in
  * comparison to [[org.apache.spark.sql.sources.DropRelation]] for example).
  *
  * Note that the view will also be will be dropped from Spark catalog as well. In case the view
  * does not exist then an exception will be thrown unless ''allowNotExisting'' as set to true.
  *
  * Moreover dropping the view will not cascade to any referenced table.
  *
  * @param viewIdentifier The view identifier
  * @param provider The name of the data source that should provide support for
  *                 persisted views.
  * @param options The view options.
  * @param allowNotExisting Determine what to do if a view does not exist in the data source
  *                         catalog. If set to true nothing will happen however if it is set to
  *                         false then an exception will be thrown.
  */
private[sql]
case class DropPersistentViewRunnableCommand(viewIdentifier: TableIdentifier,
                                             provider: String,
                                             options: Map[String, String],
                                             allowNotExisting: Boolean)
  extends RunnableCommand {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()

    dataSource match {
      case viewProvider: ViewProvider =>
        // it might happen that the user did not register the views yet in Spark catalog. Therefor
        // we will not throw if the view does not exist in Spark catalog, however if it exists we
        // will drop it.
        if(sqlContext.catalog.tableExists(viewIdentifier.toSeq)) {
          sqlContext.catalog.unregisterTable(viewIdentifier.toSeq)
        }

        viewProvider.dropView(sqlContext, viewIdentifier.toSeq,
          new CaseInsensitiveMap(options), allowNotExisting)

        Seq.empty
      case _ => throw new RuntimeException(s"The provided data source $provider does not support" +
        "dropping persisted views.")
    }
  }
}
