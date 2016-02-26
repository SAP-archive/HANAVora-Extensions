package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, PersistedDimensionView}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.execution.datasources.SqlContextAccessor._
import org.apache.spark.sql.sources.DimensionViewProvider
import org.apache.spark.sql.sources.sql.DimensionView
import org.apache.spark.sql.{Row, SQLContext}

/**
 * Pushes down a dimension view creation and persistence to the given data source.
 *
 * Note that the dimension view will also be registered as usual in the Spark catalog as well.
 *
 * Also note that even if the dimension view statement contains relations that do not exist in the
 * data source catalog we can still store it there. (in case of Vora for example, the dimension view
 * might reference a table in a HANA system which is ok). So when the dimension view is fetched from
 * the data source back in Spark and if the dimension view contains references to relations that do
 * not exist anymore then the execution of the dimension view will fail.
 *
 * @param viewIdentifier The dimension view identifier
 * @param provider The name of the data source that should provide support for
 *                 persisted views.
 * @param options The dimension view options.
 * @param allowExisting Determine what to do if a dimension view with the same name already exists
 *                      in the data source catalog. It set to true nothing will happen however if it
 *                      is set to false then an exception will be thrown.
 */
private[sql]
case class CreatePersistentDimensionViewRunnableCommand(viewIdentifier: TableIdentifier,
                                               plan: LogicalPlan,
                                               provider: String,
                                               options: Map[String, String],
                                               allowExisting: Boolean)
  extends RunnableCommand {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = plan match {

    case PersistedDimensionView(child) =>
      val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()
      dataSource match {

        case viewProvider: DimensionViewProvider =>
          viewProvider.createDimensionView(sqlContext, DimensionView(viewIdentifier, plan),
            new CaseInsensitiveMap(options), allowExisting)

          sqlContext.registerRawPlan(child, viewIdentifier.table)
          Seq.empty

        case _ => throw new RuntimeException(s"The provided data source $provider " +
          s"does not support creating and persisting dimension views.")
      }

    case _ => throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
      s"expects a ${PersistedDimensionView.getClass.getSimpleName} plan")
  }
}


