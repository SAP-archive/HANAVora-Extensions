package org.apache.spark.sql.currency.basic

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.currency._
import org.apache.spark.sql.util.ValidatingPropertyMap._

import scala.util.Try

protected object BasicCurrencyConversionConfig {

  /**
    * Reads all environment options from the environment, overwriting
    * the defaults in the following order:
    *
    * HARD_DEFAULTS -> SPARK_CONTEXT_PROPS -> SQL_CONTEXT_PROPS
    *
    * @return A [[BasicCurrencyConversionConfig]] instance
    *
    */
  def fromEnvironment(): BasicCurrencyConversionConfig = {
    val sparkContext = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sparkContext)
    val env = DEFAULT_OPTIONS_MAP ++ sparkContext.getConf.getAll.toMap ++ sqlContext.getAllConfs
    val props = env.filter { case (key, _) => key.startsWith(CONF_PREFIX) }.map {
      case (key, value) => (key.replace(CONF_PREFIX, "").toLowerCase, value)
    }
    BasicCurrencyConversionConfig(
      ratesTable = props.getString(PARAM_SOURCE_TABLE_NAME),
      allowInverse = props(PARAM_ALLOW_INVERSE).toBoolean,
      errorHandling = props.getString(PARAM_ERROR_HANDLING),
      doUpdate = props(PARAM_DO_UPDATE).toBoolean
    )
  }
}

/**
  * Holds all environment options needed for basic currency conversion
  *
  * @param ratesTable The table name of a data frame holding the rates table
  * @param allowInverse Allow inverse rate lookup. See [[BasicCurrencyConversion]]
  * @param errorHandling Specifies the error handling strategy. See [[BasicCurrencyConversion]]
  * @param doUpdate A flag indicating that the rates table should be updated
  */
protected case class BasicCurrencyConversionConfig(
    ratesTable: String,
    allowInverse: Boolean,
    errorHandling: String,
    doUpdate: Boolean)

/**
  * Manages the stateful basic currency conversion function in SparkSQL.
  * See [[CurrencyConversionFunction]] for details.
  *
  */
object BasicCurrencyConversionFunction extends CurrencyConversionFunction {

  private val ratesMap: BasicCurrencyConversion.RatesMap = new BasicCurrencyConversion.RatesMap()
  private var currentRatesTable: Option[String] = None

  def getExpression(children: Seq[Expression]): Expression = {
    val config = BasicCurrencyConversionConfig.fromEnvironment()
    updateRatesMapIfNecessary(config)
    val conversion =
      new BasicCurrencyConversion(ratesMap, config.allowInverse, config.errorHandling)
    BasicCurrencyConversionExpression(conversion, children)
  }

  /**
    * Updates the `ratesMap` if either the source input changes, or the
    * `doUpdate` flag has been set.
    *
    * @param config A [[BasicCurrencyConversionConfig]] instance
    */
  private def updateRatesMapIfNecessary(config: BasicCurrencyConversionConfig): Unit =
    synchronized {
      val ratesTable = Some(config.ratesTable)
      if (currentRatesTable != ratesTable || config.doUpdate) {
        ratesMap.clear()
        currentRatesTable = ratesTable
        val sparkContext = SparkContext.getOrCreate()
        val sqlContext = SQLContext.getOrCreate(sparkContext)
        sqlContext.setConf(CONF_PREFIX + PARAM_DO_UPDATE, DO_UPDATE_FALSE)
        updateRatesMapByTable(ratesTable.get, sqlContext)
      }
    }

  /**
    * Updates the `ratesMap` from the content of a given table name
    *
    * @param ratesTable The name of the table
    * @param sqlContext The current sqlContext
    */
  private def updateRatesMapByTable(ratesTable: String, sqlContext: SQLContext): Unit = {
    val ratesTableData = sqlContext.sql(s"SELECT * FROM $ratesTable").collect()
    ratesTableData.foreach { row =>
      val from = row.getString(0)
      val to = row.getString(1)
      val date = row.getString(2).replaceAll("-", "").toInt
      val rate =
        Try(row.getDecimal(3)).recover {
          case ex: ClassCastException => new java.math.BigDecimal(row.getDouble(3))
        }.get
      ratesMap.put((from, to), date, rate)
    }
  }
}
