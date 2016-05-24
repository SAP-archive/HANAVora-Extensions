package org.apache.spark.sql.currency.erp

import org.apache.spark.sql.SQLContext

object ERPCurrencyConversionTestUtils {

  /**
    * Registers sample ERP conversion tables using the given table
    * mapping (TableID -> registered table name).
    * @param sqlContext The current sqlContext.
    * @param tables The table mapping (TableID -> registered table name) where
    *               TableIDs are of the list (tcurx, tcurv, tcurf, tcurr, tcurn)
    * @param parallelism Number of partitions used to create the data (if possible).
    *
    */
  def createERPTables(sqlContext: SQLContext,
                      tables: Map[String, String],
                      parallelism: Int): Unit = {
    /*
     * These are highly specific values taken from the HANA test cases
     * to test the integration into Spark.
     * For real tests of numeric results see the ERP project.
     */
    val tcurx = List(("USD", 2), ("EUR", 2))
    val tcurv = List(("000", "M", "1", "", "0", "", "", "0", "0"))
    val tcurf = List(("000", "M", "USD", "EUR", "79839898", 1, 1, "", ""),
                     ("000", "M", "EUR", "USD", "79839898", 1, 1, "", ""))
    val tcurr = List(("000", "M", "USD", "EUR", "79839898", 0.7, 1, 1),
                     ("000", "M", "EUR", "USD", "79839898", 1.2, 1, 1))
    val tcurn = List(("000", "M", "USD", "EUR", "79839898", ""),
                     ("000", "M", "EUR", "USD", "79839898", ""))

    val tcurxRDD = sqlContext.sparkContext.parallelize(tcurx, parallelism)
    val tcurvRDD = sqlContext.sparkContext.parallelize(tcurv, parallelism)
    val tcurfRDD = sqlContext.sparkContext.parallelize(tcurf, parallelism)
    val tcurrRDD = sqlContext.sparkContext.parallelize(tcurr, parallelism)
    val tcurnRDD = sqlContext.sparkContext.parallelize(tcurn, parallelism)

    sqlContext.createDataFrame(tcurxRDD).registerTempTable(tables("tcurx"))
    sqlContext.createDataFrame(tcurvRDD).registerTempTable(tables("tcurv"))
    sqlContext.createDataFrame(tcurfRDD).registerTempTable(tables("tcurf"))
    sqlContext.createDataFrame(tcurrRDD).registerTempTable(tables("tcurr"))
    sqlContext.createDataFrame(tcurnRDD).registerTempTable(tables("tcurn"))
  }

  def dropERPTables(sqlContext: SQLContext, tables: Map[String, String]): Unit = {
    tables.foreach { case (tableID, tableName) =>
      sqlContext.sql(s"DROP TABLE IF EXISTS $tableName")
    }
  }

}
