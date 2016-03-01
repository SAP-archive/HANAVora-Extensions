package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.CubeViewProvider
import org.apache.spark.sql.sources.sql.CubeView
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{PersistedCubeView, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import SqlContextAccessor._

/**
 * Pushes down a cube view creation and persistence to the given data source.
 *
 * Note that the cube view will also be registered as usual in the Spark catalog as well.
 *
 * Also note that even if the cube view statement contains relations that do not exist in the
 * data source catalog we can still store it there. (in case of Vora for example, the cube view
 * might reference a table in a HANA system which is ok). So when the cube view is fetched from
 * the data source back in Spark and if the cube view contains references to relations that do
 * not exist anymore then the execution of the cube view will fail.
 *
 * @param viewIdentifier The cube view identifier
 * @param provider The name of the data source that should provide support for
 *                 persisted views.
 * @param options The cube view options.
 * @param allowExisting Determine what to do if a cube view with the same name already exists
 *                      in the data source catalog. It set to true nothing will happen however if it
 *                      is set to false then an exception will be thrown.
 */
private[sql]
case class CreatePersistentCubeViewRunnableCommand(viewIdentifier: TableIdentifier,
                                                        plan: LogicalPlan,
                                                        provider: String,
                                                        options: Map[String, String],
                                                        allowExisting: Boolean)
  extends RunnableCommand {

  override def output: Seq[Attribute] = Seq.empty
  override def children: Seq[LogicalPlan] = Seq.empty

  override def run(sqlContext: SQLContext): Seq[Row] = plan match {

    case PersistedCubeView(child) =>
      val dataSource: Any = ResolvedDataSource.lookupDataSource(provider).newInstance()
      dataSource match {

        case viewProvider: CubeViewProvider =>

          // use catalog tableExists method as it takes into account whether Spark SQL
          // is case-sensitive or not
          if (sqlContext.catalog.tableExists(viewIdentifier.toSeq)) {
            if (!allowExisting) {
              sys.error(s"Relation ${viewIdentifier.toSeq} already exists")
            }
          }

          viewProvider.createCubeView(sqlContext, CubeView(viewIdentifier, plan),
            new CaseInsensitiveMap(options), allowExisting)

          sqlContext.registerRawPlan(child, viewIdentifier.table)
          Seq.empty

        case _ => throw new RuntimeException(s"The provided data source $provider " +
          s"does not support creating and persisting cube views.")
      }

    case _ => throw new IllegalArgumentException(s"the ${getClass.getSimpleName} " +
      s"expects a ${PersistedCubeView.getClass.getSimpleName} plan")
  }
}

