package org.apache.spark.sql.currency.erp

import com.sap.hl.currency.api.CurrencyConversionOptions
import com.sap.hl.currency.erp.{ERPConversionData, ERPDataLoader}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.currency._
import org.apache.spark.sql.util.ValidatingPropertyMap._
import org.apache.spark.{Logging, SparkContext}

import scala.util.control.NonFatal

/**
  * This object holds the state for the ERP currency conversion. Before each call, this
  * object's update method is invoked and the latest rates and configurations are shipped to the
  * spark workers.
  */
object ERPCurrencyConversionFunction
    extends CurrencyConversionFunction
    with Logging {

  case class ERPCurrencyConversionSource(
      tablePrefix: String,
      tableNames: Map[String, String]) {

    def tableMap: Map[String, String] =
      tableNames.map { case (tabKey, tabName) => (tabKey, tablePrefix + tabName) }
  }

  case class ERPCurrencyConversionOptions(
      accuracy: String,
      dateFormat: String,
      errorHandling: String,
      lookup: String,
      steps: String) {

    def toExternalOptionObject: CurrencyConversionOptions = {
      CurrencyConversionOptions(
        accuracy = accuracy,
        date_format = dateFormat,
        error_handling = errorHandling,
        lookup = lookup,
        steps = steps
      )
    }
  }

  object ERPCurrencyConversionConfig {

    def fromEnvironment(): ERPCurrencyConversionConfig = {
      val sparkContext = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sparkContext)
      val env = DEFAULT_OPTIONS_MAP ++ sparkContext.getConf.getAll.toMap ++ sqlContext.getAllConfs
      val props = env
        .filter { case (key, _) => key.startsWith(CONF_PREFIX) }
        .map { case (key, value) => (key.replace(CONF_PREFIX, "").toLowerCase, value) }

      val tableNames = PARAM_TABLE_NAMES.map(name => name -> props.getString(name)).toMap

      val sourceConfig = ERPCurrencyConversionSource(
        tablePrefix = props.getString(PARAM_TABLE_PREFIX),
        tableNames = tableNames
      )
      val options = ERPCurrencyConversionOptions(
        accuracy = props.getString(PARAM_ACCURACY),
        dateFormat = props.getString(PARAM_DATE_FORMAT),
        errorHandling = props.getString(PARAM_ERROR_HANDLING),
        lookup = props.getString(PARAM_LOOKUP),
        steps = props.getString(PARAM_STEPS)
      )
      val doUpdate = props.get(PARAM_DO_UPDATE).get.toBoolean
      val config = ERPCurrencyConversionConfig(sourceConfig, options, doUpdate)
      log.debug(s"Loaded currency configuration from environment: $config")
      config
    }
  }

  case class ERPCurrencyConversionConfig(
      source: ERPCurrencyConversionSource,
      options: ERPCurrencyConversionOptions,
      doUpdate: Boolean)

  private[erp] var erpData: Option[ERPConversionData] = None
  private var sourceConfig: Option[ERPCurrencyConversionSource] = None

  def getExpression(children: Seq[Expression]): Expression = {
    log.info("Creating currency expression")
    val config = ERPCurrencyConversionConfig.fromEnvironment()
    updateDataIfNecessary(config)
    ERPCurrencyConversionExpression(erpData.get, config.options, children)
  }

  private def updateDataIfNecessary(config: ERPCurrencyConversionConfig): Unit = {
    if (Some(config.source) != sourceConfig || config.doUpdate) {
      val msStart = System.currentTimeMillis()
      val updateCause = if (config.doUpdate) "do_update forced" else "source config changed"
      log.info(s"Updating currency function state because $updateCause")
      sourceConfig = Some(config.source)
      val sparkContext = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sparkContext)
      erpData = Some(loadDataFromTables(config.source.tableMap, sqlContext))
      log.debug("Erp tables successfully loaded - Setting do_update to false")
      sqlContext.setConf(CONF_PREFIX + PARAM_DO_UPDATE, DO_UPDATE_FALSE)
      val msNeeded = System.currentTimeMillis() - msStart
      log.info(s"Updating currency function finished - Needed: $msNeeded ms")
    }
  }

  private def loadDataFromTables(tableMap: Map[String, String],
                                 sqlContext: SQLContext): ERPConversionData = {
    // TODO(MD): converting to string here and back from string in ERPDataLoader is not optimal!
    // TODO(MD): => move this logic into [[ERPDataLoader]]
    val iterators = ERPDataLoader.tableNames.mapValues { tableName =>
      val configuredName = tableMap(tableName)
      try {
        log.debug(s"Loading erp table [$configuredName] and wrap it as iterator")
        sqlContext.sql(s"SELECT * FROM $configuredName").collect().iterator.map { row =>
          (0 until row.length).map(row.apply(_).toString)
        }
      }
      catch {
        case NonFatal(ex) =>
          val msg = s"Could not load erp data table [$configuredName]; Stop loading"
          log.warn(msg)
          throw new CurrencyConversionException(msg, ex)
      }
    }
    log.debug("Read erp data from iterator")
    ERPDataLoader.createFromTableMap(iterators)
  }
}

