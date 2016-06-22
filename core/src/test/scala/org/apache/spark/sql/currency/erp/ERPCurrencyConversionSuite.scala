package org.apache.spark.sql.currency.erp

import org.apache.spark.sql.currency.{CurrencyConversionException, CurrencyConversionFunction, ERPDataRow}
import org.apache.spark.sql.hive.SapHiveContext
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.scalatest.{BeforeAndAfterEach, FunSuite, ShouldMatchers}

import scala.annotation.tailrec

/**
  * ERP currency conversion tests (rates table source agnostic).
  * This test focus on the integration into spark.
  * Numeric tests of correct conversion happen in the ERP project.
  */
class ERPCurrencyConversionSuite
  extends FunSuite
  with GlobalSapSQLContext
  with ShouldMatchers
  with BeforeAndAfterEach {


  def throwsNicely[T](action: => T): Unit = {
    val err = intercept [Exception] {action}
    assert(
      containsCurrencyConversionException(err),
      "An exception was thrown, but it did not contain hints to currency conversion")
  }

  /**
    * Check whether the give nexception contains a CurrencyConversionException as a cause.
    *
    * Note: HiveContext silently swallows exceptions during expression generation and reports the
    * corresponding UDF as absent. Therefore, we log an error in the builder so that the user can
    * see from the logs what went wrong. However, there will be no [[CurrencyConversionException]]
    * when HiveContext is used - in this case this function returns true.
    *
    * @param err exception to analyze
    * @return
    */
  @tailrec
  private def containsCurrencyConversionException(err: Throwable): Boolean = {
    val isHiveContext = sqlContext.isInstanceOf[SapHiveContext]
    val earlyAbort = err.isInstanceOf[CurrencyConversionException] || isHiveContext

    (earlyAbort, Option(err.getCause)) match {
      case (true, _) => true
      case (maybe, None) => maybe
      case (_, Some(cause)) => containsCurrencyConversionException(cause)
    }
  }

  val FUNCTION_NAME =
    CurrencyConversionFunction.functions.filter(_._2 == ERPCurrencyConversionFunction).head._1

  /* Used to test the different behaviour of the context implementations
   * without running the tests in the Maven test phase.
   * In the Maven test phase both [SapSQLContext] and [SapHiveContext] are used.
   * Uncomment to test against the [SapHiveContext], e.g. when running from IntelliJ. Otherwise,
   * the [SapSQLContext] is used. Left here to ease development.
   */
  // System.setProperty("test.with.hive.context", "true")

  val ERP_TABLE_PREFIX = "test_erp_prefix_"
  val ERP_TABLE_MAPPING = Map[String, String](
    "tcurx" -> "x",
    "tcurv" -> "v",
    "tcurf" -> "f",
    "tcurr" -> "r",
    "tcurn" -> "n"
  )
  val ERP_TABLE_MAPPING_WITH_PREFIX = Map[String, String](
    "tcurx" -> (ERP_TABLE_PREFIX + "x"),
    "tcurv" -> (ERP_TABLE_PREFIX + "v"),
    "tcurf" -> (ERP_TABLE_PREFIX + "f"),
    "tcurr" -> (ERP_TABLE_PREFIX + "r"),
    "tcurn" -> (ERP_TABLE_PREFIX + "n")
  )

  val DATA_TABLE = "erp_conversion_table"

  val QUERY_WITH_FIX_ARGS = s"""
      |SELECT amount, spark_partition_id(), $FUNCTION_NAME("000", "M", amount, from, to,
      |date) FROM $DATA_TABLE
    """.stripMargin

  val QUERY_WITH_UNKNOWN_CLIENT = s"""
       |SELECT amount, spark_partition_id(), $FUNCTION_NAME("999", "M", amount, from, to,
       |date) FROM $DATA_TABLE
    """.stripMargin

  val QUERY = s"""
       |SELECT amount, spark_partition_id(), $FUNCTION_NAME(client, method, amount, from, to,
       |date) FROM $DATA_TABLE
    """.stripMargin

  val PARALLELISM = 2

  def setOption(key: String, value: String): Unit =
    sqlContext.setConf(CONF_PREFIX + key, value)

  def setERPTableMapping(tableMapping: Map[String, String]): Unit = {
    sqlContext.sql(s"SET ${CONF_PREFIX}tcurx = ${tableMapping("tcurx")}")
    sqlContext.sql(s"SET ${CONF_PREFIX}tcurv = ${tableMapping("tcurv")}")
    sqlContext.sql(s"SET ${CONF_PREFIX}tcurf = ${tableMapping("tcurf")}")
    sqlContext.sql(s"SET ${CONF_PREFIX}tcurr = ${tableMapping("tcurr")}")
    sqlContext.sql(s"SET ${CONF_PREFIX}tcurn = ${tableMapping("tcurn")}")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    ERPCurrencyConversionTestUtils
      .createERPTables(sqlContext, ERP_TABLE_MAPPING, PARALLELISM)
    ERPCurrencyConversionTestUtils
      .createERPTables(sqlContext, ERP_TABLE_MAPPING_WITH_PREFIX, PARALLELISM)
    setERPTableMapping(ERP_TABLE_MAPPING)

    val rows = (1 to 10).map { i =>
      ERPDataRow("000", "M", i + 1000.0, "EUR", "USD", "2016-01-%02d".format(i + 10))
    }
    val dataRDD = sc.parallelize(rows, PARALLELISM)
    sqlContext.createDataFrame(dataRDD).registerTempTable(DATA_TABLE)

    // set default options
    setOption(PARAM_DO_UPDATE, DO_UPDATE_TRUE)
    setOption(PARAM_ACCURACY, "compatibility")
    setOption(PARAM_DATE_FORMAT, "auto_detect")
    setOption(PARAM_LOOKUP, "regular")
    setOption(PARAM_STEPS, "shift,convert")
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
  }

  override def afterEach(): Unit = {
    sqlContext.sql(s"DROP TABLE $DATA_TABLE")
    ERPCurrencyConversionTestUtils.dropERPTables(sqlContext, ERP_TABLE_MAPPING)
    ERPCurrencyConversionTestUtils.dropERPTables(sqlContext, ERP_TABLE_MAPPING_WITH_PREFIX)
    super.afterEach()
  }

  test("smoke test of all queries with different error modes") {
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_NULL)
    sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_ (2))
    sqlContext.sql(QUERY).collect().map(_ (2))
    sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().map(_ (2))

    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_KEEP)
    sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_ (2))
    sqlContext.sql(QUERY).collect().map(_ (2))
    sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().map(_ (2))

    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
    sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_ (2))
    sqlContext.sql(QUERY).collect().map(_ (2))
    throwsNicely {
      sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().map(_ (2))
    }
  }

  test("bad config fails") {
    val optionName = "date_format"
    val default = "auto_detect"

    setOption(optionName, "DEFINITELY_INVALID")

    throwsNicely {
      sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_ (2))
    }

    // this should work again
    setOption(optionName, default)
    sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_ (2))
  }

  test("error handling works") {
    // default should fail
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
    throwsNicely {
      sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().map(_ (2))
    }

    // bogus should fail as well
    setOption(PARAM_ERROR_HANDLING, "BOGUS VALUE")
    throwsNicely {
      sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().map(_ (2))
    }

    // keep should work
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_KEEP)
    sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().foreach {
      case Row(left: Double, _, right: Double, _ *) => left should be (right)
    }

    // null should also work
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_NULL)
    sqlContext.sql(QUERY_WITH_UNKNOWN_CLIENT).collect().forall(_.isNullAt(2)) should be (true)
  }

  test("conversion is distributed") {
    sqlContext.sql(QUERY_WITH_FIX_ARGS).collect().map(_.getInt(1)).toSet.size should be(PARALLELISM)
  }

  test("do_update") {
    sqlContext.sql(QUERY).collect().map(_ (2))

    // should work after tables have been deleted
    ERPCurrencyConversionTestUtils.dropERPTables(sqlContext, ERP_TABLE_MAPPING)
    sqlContext.sql(QUERY).collect().map(_ (2))

    setOption(PARAM_DO_UPDATE, DO_UPDATE_TRUE)

    // after we force an update, conversion should fail because of missing tables
    // the type of error thrown depends on the context: SQLContext will propagate
    // [[CurrencyConversionException]], HiveContext will wrap it in an [[AnalysisException]].
    throwsNicely {
      val rows = sqlContext.sql(QUERY).collect()
    }

    // should be true since unsuccessful
    sqlContext.getConf(CONF_PREFIX + PARAM_DO_UPDATE) should be (DO_UPDATE_TRUE)

    // now it should work again
    ERPCurrencyConversionTestUtils.createERPTables(sqlContext, ERP_TABLE_MAPPING, PARALLELISM)
    sqlContext.sql(QUERY).collect().map(_ (2))
    sqlContext.getConf(CONF_PREFIX + PARAM_DO_UPDATE) should be (DO_UPDATE_FALSE)
  }

  test("caching and switching by prefix works") {
    sqlContext.sql(QUERY).collect().map(_ (2))
    val erpDataRef = ERPCurrencyConversionFunction.conversionFunctionHolder.get
    sqlContext.sql(QUERY).collect().map(_ (2))
    ERPCurrencyConversionFunction.conversionFunctionHolder.get should be (erpDataRef)

    setOption(PARAM_TABLE_PREFIX, ERP_TABLE_PREFIX)
    sqlContext.sql(QUERY).collect().map(_ (2))
    ERPCurrencyConversionFunction.conversionFunctionHolder.get should not be erpDataRef
  }

  test("fails meaningfully if erp library is missing") {
    val backup_modulename = ERPConversionLoader.MODULE_NAME
    setOption(PARAM_DO_UPDATE, DO_UPDATE_TRUE)
    ERPConversionLoader.MODULE_NAME = "invalid.BOGUS"
    try {
      throwsNicely {
        sqlContext.sql(QUERY).collect().map(_ (2))
      }
    } finally {
      ERPConversionLoader.MODULE_NAME = backup_modulename
    }
  }

  test("fails meaningfully if erp library has unexpected version") {
    val backup_modulename = ERPConversionLoader.MODULE_NAME
    setOption(PARAM_DO_UPDATE, DO_UPDATE_TRUE)
    ERPConversionLoader.MODULE_NAME =
      "org.apache.spark.sql.currency.erp.UnexpectedConversionProvider"
    try {
      throwsNicely {
        sqlContext.sql(QUERY).collect().map(_ (2))
      }
    } finally {
      ERPConversionLoader.MODULE_NAME = backup_modulename
    }
  }
}
