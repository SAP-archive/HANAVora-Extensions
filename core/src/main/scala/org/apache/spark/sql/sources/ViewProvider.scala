package org.apache.spark.sql.sources

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.sources.sql.View

/**
  * An interface that marks a data source as a view provider.
  * It allows the user to push down views to the target data source where the view's logical plan is
  * serialized in the data source catalog.
  *
  * TODO (YH) we should discuss whether we want to support updating the view.
*/
trait ViewProvider {

  /**
    * Save the view in the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param view The view.
    * @param options The options of the view.
    * @param allowExisting True if no error should be thrown if the view already exits, otherwise
    *                      false.
    */
  def createView(sqlContext: SQLContext,
                 view: View,
                 options: Map[String, String],
                 allowExisting: Boolean): Unit


  /**
    * Drops the view from the catalog of the data source.
    *
    * @param sqlContext The SQL Context.
    * @param view The view identifier of form [db]?.[name]. Hint: The reason behind this is that
    *             [[TableIdentifier]] is package private.
    * @param options The options.
    * @param allowNotExisting If true then no exception will be thrown if the view does not exist,
    *                      otherwise an exception will be thrown.
    */
  def dropView(sqlContext: SQLContext,
               view: Seq[String],
               options: Map[String, String],
               allowNotExisting: Boolean): Unit
}
