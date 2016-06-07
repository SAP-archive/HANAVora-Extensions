package org.apache.spark.sql.currency.erp

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.currency.{CurrencyConversionException, _}
import org.apache.spark.sql.currency.erp.ERPConversionLoader.ConversionFunction
import org.apache.spark.sql.util.ValidatingPropertyMap._
import org.apache.spark.{Logging, SparkContext}

import scala.language.reflectiveCalls
import scala.reflect.internal.MissingRequirementError
import scala.reflect.runtime._
import scala.util.Try
import scala.util.control.NonFatal

/**
  * This object holds the state for the ERP currency conversion. Before each call, this
  * object's update method is invoked and the latest rates and configurations are shipped to the
  * spark workers.
  */
object ERPCurrencyConversionFunction extends CurrencyConversionFunction with Logging {

  private val DEFAULT_TABLES = Map("configuration" -> "tcurv",
                                   "precisions" -> "tcurx",
                                   "notation" -> "tcurn",
                                   "rates" -> "tcurr",
                                   "prefactors" -> "tcurf")

  case class ERPCurrencyConversionSource(tablePrefix: String, tableNames: Map[String, String]) {

    def tableMap: Map[String, String] =
      tableNames.map { case (tabKey, tabName) => (tabKey, tablePrefix + tabName) }
  }

  case class ERPCurrencyConversionOptions(accuracy: String,
                                          dateFormat: String,
                                          errorHandling: String,
                                          lookup: String,
                                          steps: String) {

    def toMap: Map[String, String] = {
      Map("accuracy" -> accuracy,
          "date_format" -> dateFormat,
          "error_handling" -> errorHandling,
          "lookup" -> lookup,
          "steps" -> steps)
    }
  }

  object ERPCurrencyConversionConfig {

    def fromEnvironment(): ERPCurrencyConversionConfig = {
      val sparkContext = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sparkContext)
      val env = DEFAULT_OPTIONS_MAP ++ sparkContext.getConf.getAll.toMap ++ sqlContext.getAllConfs
      val props = env.filter { case (key, _) => key.startsWith(CONF_PREFIX) }.map {
        case (key, value) => (key.replace(CONF_PREFIX, "").toLowerCase, value)
      }

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

  case class ERPCurrencyConversionConfig(source: ERPCurrencyConversionSource,
                                         options: ERPCurrencyConversionOptions,
                                         doUpdate: Boolean)

  private[erp] var conversionFunctionHolder: Option[ConversionFunction] = None
  private var sourceConfig: Option[ERPCurrencyConversionSource] = None
  private var options: Option[ERPCurrencyConversionOptions] = None

  def getExpression(children: Seq[Expression]): Expression = {
    log.info("Creating currency expression")
    val config = ERPCurrencyConversionConfig.fromEnvironment()

    // update if either requested by user via SQL option, or if config changed
    (sourceConfig, options, config.doUpdate) match {
      case (Some(config.source), Some(config.options), false) =>
      case _ => updateData(config)
    }
    ERPCurrencyConversionExpression(conversionFunctionHolder.get, children)
  }

  private def updateData(config: ERPCurrencyConversionConfig): Unit = {
    val updateCause = if (config.doUpdate) "do_update is set" else "source config changed"
    log.info(s"Updating currency function state because $updateCause")

    val sparkContext = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sparkContext)

    // get data iterators from tables
    val dataIterators = DEFAULT_TABLES.mapValues { tableName =>
      val configuredName = config.source.tableMap(tableName)
      try {
        log.debug(s"Loading erp table [$configuredName] and wrap it as iterator")
        sqlContext.sql(s"SELECT * FROM $configuredName").collect().iterator.map { row =>
          (0 until row.length).map(row.apply(_).toString)
        }
      } catch {
        case NonFatal(ex) =>
          val msg = s"could not load erp data table '$configuredName'"
          throw new CurrencyConversionException(msg, ex)
      }
    }

    val closure = ERPConversionLoader.createInstance(config.options.toMap, dataIterators)
    log.debug("Erp tables successfully loaded.")
    sqlContext.setConf(CONF_PREFIX + PARAM_DO_UPDATE, DO_UPDATE_FALSE)

    sourceConfig = Some(config.source)
    options = Some(config.options)
    conversionFunctionHolder = Some(closure)
  }
}

/**
  * Glue class to load 'com.sap.hl.currency'.
  */
protected[erp] object ERPConversionLoader {

  var MODULE_NAME = "com.sap.hl.currency.erp.ERPConversionProvider"

  type Options = Map[String, String]
  type Tables = Map[String, Iterator[Seq[String]]]

  type ConversionFunction = (Option[String], Option[String], Option[String], Option[String],
                             Option[String]) => (Option[Double] => Try[Option[Double]])

  // we need to switch this off, otherwise we will not be able to load duck-typed
  // scalastyle:off structural.type
  type DuckType = {
    def getConversion(options: Options, conversionData: Tables): ConversionFunction
  }

  /**
    * Loads the ERP conversion function at runtime. If the 'com.sap.hl.currency' jar is not
    * provided, or if there are discrepancies between the packaged object and the expected
    * closure format, a [[CurrencyConversionException]] is thrown.
    *
    * @param options the ERP conversion options (possible keys: accuracy, error_handling, lookup,
    *                steps, date_format)
    * @param tables a mapping of ERP table pretty names (see
    *               [[ERPCurrencyConversionFunction.DEFAULT_TABLES]]) to data iterators
    * @return the closure representing the readily configured ERP conversion
    */
  def createInstance(options: Options, tables: Tables): ConversionFunction = {

    val mirror = universe.runtimeMirror(getClass.getClassLoader)
    try {
      val module = mirror.staticModule(MODULE_NAME)
      val obj = mirror.reflectModule(module)

      obj.instance.asInstanceOf[DuckType].getConversion(options, tables)
    } catch {
      case err: MissingRequirementError =>
        val msg = s"this conversion method requires module '$MODULE_NAME'"
        throw new CurrencyConversionException(msg, err)
      case err: NoSuchMethodException =>
        val msg = s"matching object in '$MODULE_NAME' is incompatible (possibly wrong version)"
        throw new CurrencyConversionException(msg, err)
    }
  }
}
