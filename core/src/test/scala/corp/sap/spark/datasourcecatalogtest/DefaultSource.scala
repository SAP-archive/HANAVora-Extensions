package corp.sap.spark.datasourcecatalogtest

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.DatasourceCatalog

/**
 * Used to test the SHOW DATASOURCETABLES command!
 */

object DefaultSource {

  var tablesSeq : Seq[String] = Seq()
  var passedOptions : Map[String,String] = Map.empty

  def addTable(tableName : String) : Unit  = {
    tablesSeq = tablesSeq :+ tableName
  }

  def reset(): Unit ={
    tablesSeq = Seq()
    passedOptions = Map.empty
  }
}

class DefaultSource extends DatasourceCatalog{

  override def getTableNames(sqlContext: SQLContext,
                             parameters: Option[Map[String, String]]): Seq[String]
  = {
    DefaultSource.passedOptions = DefaultSource.passedOptions ++ parameters.getOrElse(Map.empty)
    DefaultSource.tablesSeq
  }

}
