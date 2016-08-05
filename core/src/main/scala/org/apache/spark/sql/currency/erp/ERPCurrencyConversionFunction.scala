package org.apache.spark.sql.currency.erp

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.currency.{CurrencyConversionException, _}
import org.apache.spark.sql.currency.erp.ERPConversionLoader.RConversionOptionsCurried
import org.apache.spark.sql.util.ValidatingPropertyMap._
import org.apache.spark.{Logging, SparkContext}

import scala.language.reflectiveCalls
import scala.reflect.runtime._
import scala.util.{Failure, Success, Try}
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

  case class ERPCurrencyConversionSource(
      tablePrefix: String,
      tableNames: Map[String, String],
      schema: String) {
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
          tableNames = tableNames,
          schema = props.getString(PARAM_SCHEMA)
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

  private[erp] var conversionFunctionHolder: Option[RConversionOptionsCurried] = None
  private var sourceConfig: Option[ERPCurrencyConversionSource] = None
  private var options: Option[ERPCurrencyConversionOptions] = None

  def getExpression(children: Seq[Expression]): Expression = {

    val conversionFunction = try {
      log.debug("Creating currency expression")
      val config = ERPCurrencyConversionConfig.fromEnvironment()
      // update if either requested by user via SQL option, or if config changed
      (sourceConfig, options, config.doUpdate) match {
        case (Some(config.source), Some(config.options), false) =>
        case _ => updateData(config)
      }

      // if this is not set something went wrong silently
      conversionFunctionHolder.getOrElse {
        val msg = "Could not load currency conversion data for unknown reasons"
        ERPConversionLoader.getErrorCaseFallback(new CurrencyConversionException(msg))
      }
    } catch {
      case err: CurrencyConversionException => ERPConversionLoader.getErrorCaseFallback(err)
    }

    ERPCurrencyConversionExpression(conversionFunction, children)
  }

  /**
    * Fetch the latest currency conversion config and data. This method sets the [[PARAM_DO_UPDATE]]
    * to `false`  iff options and source configuration could be read and the conversion closure
    * could be constructed. If the closure cannot be constructed, the method sets
    * [[PARAM_DO_UPDATE]] to `true` so that constructing it will be retried even when config does
    * not change (the tables could have changed in the mean time).
    * @param config
    */
  private def updateData(config: ERPCurrencyConversionConfig): Unit = {
    val updateCause = if (config.doUpdate) "do_update is set" else "source config changed"
    log.debug(s"Updating currency function state because $updateCause")

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
          val msg = s"could not load erp data table [$configuredName]"
          throw new CurrencyConversionException(msg, ex)
      }
    }

    val maybeClosure = ERPConversionLoader.createInstance(config.options.toMap, dataIterators)

    sourceConfig = Some(config.source)
    options = Some(config.options)

    maybeClosure match {
      case Success(closure) =>
        log.debug("Erp tables successfully loaded.")
        sqlContext.setConf(CONF_PREFIX + PARAM_DO_UPDATE, DO_UPDATE_FALSE)
        conversionFunctionHolder = Option(closure)
      case Failure(err) =>
        sqlContext.setConf(CONF_PREFIX + PARAM_DO_UPDATE, DO_UPDATE_TRUE)
        conversionFunctionHolder = Option(ERPConversionLoader.getErrorCaseFallback(err))

    }
  }
}

/**
  * Glue class to load 'com.sap.hl.currency'.
  */
protected[erp] object ERPConversionLoader {

  var MODULE_NAME = "com.sap.hl.currency.erp.ERPConversionProvider"

  private val moduleNotFoundMsg = s"this conversion method requires module '$MODULE_NAME'"
  private val couldNotInvokeMsg =
    s"matching object in '$MODULE_NAME' is incompatible (possibly wrong version)"
  private val setupErrorMsg =
    "Currency conversion encountered an internal error during setup (see below)"

  type RConversionAmount = Option[Double]
  type RConversionResult = Try[RConversionAmount]
  type RConversionOptions = Map[String, String]
  type RConversionData = Map[String, Iterator[Seq[String]]]

  type RConversionFixArgsCurried = RConversionAmount => RConversionResult
  type RConversionOptionsCurried =
  (Option[String], Option[String], Option[String], Option[String], Option[String])
    => RConversionFixArgsCurried
  type RConversionDataCurried = RConversionOptions => RConversionOptionsCurried
  type RConversionBuilder = RConversionData => RConversionDataCurried

  // we need to switch this off, otherwise we will not be able to load duck-typed
  // scalastyle:off structural.type
  type DuckType = {
    def getConversionBuilder: RConversionBuilder
  }
  // scalastyle:on

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
  def createInstance(options: RConversionOptions,
                     tables: RConversionData): Try[RConversionOptionsCurried] = {

    val mirror = universe.runtimeMirror(getClass.getClassLoader)

    def fail(msg: String, cause: Throwable) = {
      Failure(new CurrencyConversionException(msg, cause))
    }

    Try(mirror.staticModule(MODULE_NAME))
      .recoverWith { case NonFatal(ex) => fail(moduleNotFoundMsg, ex) }
      .map(mirror.reflectModule(_).instance.asInstanceOf[DuckType])
      .recoverWith { case NonFatal(ex) => fail(couldNotInvokeMsg, ex) }
      .map(_.getConversionBuilder)
      .recoverWith {
        case ex: NoSuchMethodException => fail(couldNotInvokeMsg, ex)
        case NonFatal(ex) => fail(setupErrorMsg, ex)
      }
      .map(_(tables)(options))
      .recoverWith {case NonFatal(ex) => fail(setupErrorMsg, ex)}
  }

  /**
    * Create a closure to distribute in case something went wrong. We prefer distributing an error
    * instead of failing early for the following reasons:
    *
    *   - HiveContext in Spark 1.6 swallows errors during function lookup, so we cannot inform the
    *     user about misconfigurations
    *   - even if there is no currency conversion library, pushing down might still work
    *
    * @param cause the underlying error (will be wrapped in a [[CurrencyConversionException]]
    * @return
    */
  def getErrorCaseFallback(cause: Throwable): RConversionOptionsCurried = {
    {
      (a: Option[String],
       b: Option[String],
       c: Option[String],
       d: Option[String],
       e: Option[String]) =>
        (x: Option[Double]) =>
          val msg = """
                      |Currency conversion could not be initialized, and
                      |no push-down alternatives are available.
                    """.stripMargin.trim
          throw new CurrencyConversionException(msg, cause)
    }
  }
}
