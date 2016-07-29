package org.apache.spark.sql.currency.basic

import org.apache.spark.SparkException
import org.apache.spark.sql.GlobalSapSQLContext
import org.apache.spark.sql.currency._
import org.apache.spark.sql.currency.TestUtils._
import org.scalatest.{BeforeAndAfterEach, _}



class BasicCurrencyConversionFunctionSuite
  extends FunSuite
  with GlobalSapSQLContext
  with ShouldMatchers
  with BeforeAndAfterEach {


  val FUNCTION_NAME = CurrencyConversionFunction.functions
    .filter(_._2 == BasicCurrencyConversionFunction).head._1

  val DATA_TABLE = "simple_input_table"
  val DATA_ROWS = (1 to 10).map { i =>
    DataRow(new java.math.BigDecimal(i + 1000.0), "EUR", "USD", "2016-01-%02d".format(i + 10))
  }

  // data table using DOUBLE instead of DECIMAL
  val DATA_TABLE_DOUBLE = "input_table_using_doubles"
  val DATA_ROWS_DOUBLE = (1 to 10).map { i =>
    DataRowUsingDouble(i + 1000.0, "EUR", "USD", "2016-01-%02d".format(i + 10))
  }

  // Two set of rate and expected result tables.
  val RATES_TABLE_1 = "CUSTOM_RATES_1"
  val RATE_ROWS_1 = (1 to 10).map { i =>
    RateRow("EUR", "USD", "2016-01-%02d".format(i + 10), new java.math.BigDecimal(1.0 * i))
  }
  val EXPECTED_RESULT_1 = DATA_ROWS.map(_.amount).zip(RATE_ROWS_1.map(_.rate)).map {
    case (x, r) => x.multiply(r)
  }

  val RATES_TABLE_2 = "CUSTOM_RATES_2"
  val RATE_ROWS_2 = (1 to 10).map { i =>
    RateRow("EUR", "USD", "2016-01-%02d".format(i + 10), new java.math.BigDecimal(2.0 * i))
  }
  val EXPECTED_RESULT_2 = DATA_ROWS.map(_.amount).zip(RATE_ROWS_2.map(_.rate)).map {
    case (x, r) => x.multiply(r)
  }

  // one rates table using DOUBLE type
  val RATES_TABLE_DOUBLE = "CUSTOM_RATES_1"
  val RATE_ROWS_DOUBLE = (1 to 10).map { i =>
    RateRowUsingDouble("EUR", "USD", "2016-01-%02d".format(i + 10), 1.0 * i)
  }

  // Default names for rate and expected result tables
  // for tests that only need one set of tables.
  val RATES_TABLE = RATES_TABLE_1
  val RATE_ROWS = RATE_ROWS_1
  val EXPECTED_RESULT = EXPECTED_RESULT_1

  val QUERY =
    s"""SELECT amount,
        |       spark_partition_id(),
        |       $FUNCTION_NAME(amount, from, to, date)
        |FROM   $DATA_TABLE""".stripMargin

  val QUERY_WITH_NULL =
    s"""SELECT amount,
        |       spark_partition_id(),
        |       $FUNCTION_NAME(amount, NULL, to, date)
        |FROM   $DATA_TABLE""".stripMargin

  val QUERY_FROM_DOUBLE =
    s"""SELECT amount,
        |       spark_partition_id(),
        |       $FUNCTION_NAME(amount, from, to, date)
        |FROM   $DATA_TABLE_DOUBLE""".stripMargin

  val PARALLELISM = 3

  def setOption(key: String, value: String): Unit =
    sqlContext.setConf(CONF_PREFIX + key, value)

  override def beforeEach(): Unit = {
    super.beforeEach()

    // build test tables
    val dataRDD = sc.parallelize(DATA_ROWS, PARALLELISM)
    sqlContext.createDataFrame(dataRDD).registerTempTable(DATA_TABLE)
    val dataRDDDouble = sc.parallelize(DATA_ROWS_DOUBLE, PARALLELISM)
    sqlContext.createDataFrame(dataRDDDouble).registerTempTable(DATA_TABLE_DOUBLE)
    val ratesRDD1 = sc.parallelize(RATE_ROWS_1, PARALLELISM)
    sqlContext.createDataFrame(ratesRDD1).registerTempTable(RATES_TABLE_1)
    val ratesRDD2 = sc.parallelize(RATE_ROWS_2, PARALLELISM)
    sqlContext.createDataFrame(ratesRDD2).registerTempTable(RATES_TABLE_2)
    val ratesRDDDouble = sc.parallelize(RATE_ROWS_DOUBLE, PARALLELISM)
    sqlContext.createDataFrame(ratesRDDDouble).registerTempTable(RATES_TABLE_DOUBLE)
    // set default options
    setOption(PARAM_SOURCE_TABLE_NAME, RATES_TABLE)
    // Since we (still) have the UDF as an "object",
    // we need to clean up the static status.
    setOption(PARAM_DO_UPDATE, "true")
    setOption(PARAM_ALLOW_INVERSE, "false")
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
  }

  override def afterEach(): Unit = {
    sqlContext.sql(s"DROP TABLE IF EXISTS $DATA_TABLE")
    sqlContext.sql(s"DROP TABLE IF EXISTS $RATES_TABLE_1")
    sqlContext.sql(s"DROP TABLE IF EXISTS $RATES_TABLE_2")
    super.afterEach()
  }

  test("normal currency conversion works") {
    sqlContext.sql(QUERY).collect().map(_ (2))
  }

  test("bad config fails") {

    // setting an invalid option makes currency conversion fail at runtime
    setOption(PARAM_SOURCE_TABLE_NAME, "DEFINITELY_INVALID")

    intercept[Throwable] {
      sqlContext.sql(QUERY).collect().map(_ (2))
    }

    // after setting the correct option, conversion should work again
    setOption(PARAM_SOURCE_TABLE_NAME, RATES_TABLE)

    sqlContext.sql(QUERY).collect().map(_ (2))
  }

  test("null handling works") {
    sqlContext.sql(QUERY_WITH_NULL).collect().forall(_.isNullAt(2)) should be(true)
  }

  test("values are correct") {
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
  }

  test("conversion is actually distributed") {
    sqlContext.sql(QUERY).collect().map(_.getInt(1)).toSet.size should be(PARALLELISM)
  }

  test("switch data option") {
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_1).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    sqlContext.sql(s"SET ${CONF_PREFIX + PARAM_SOURCE_TABLE_NAME} = $RATES_TABLE_2")
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_2).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    sqlContext.sql(s"SET ${CONF_PREFIX + PARAM_SOURCE_TABLE_NAME} = $RATES_TABLE_1")
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_1).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
  }

  test("do_update option") {
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_1).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    // change rows in rates_1 and check that it is not used
    sqlContext.sql(s"DROP TABLE $RATES_TABLE_1")
    val rates2 = sqlContext.sql(s"SELECT * FROM $RATES_TABLE_2")
    rates2.registerTempTable(RATES_TABLE_1)
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_1).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    // now do_update and check that values from rates_2 are used
    setOption(PARAM_DO_UPDATE, "true")
    sqlContext.sql(QUERY).collect().map(_.getDecimal(2)).zip(EXPECTED_RESULT_2).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    // do_update should automatically be set to false after it was applied
    assert(sqlContext.getConf(CONF_PREFIX + PARAM_DO_UPDATE).equalsIgnoreCase("false"))
  }

  test("allow_inverse option") {
    val INV_DATA_TABLE = "inv_data_table"
    val INV_DATA_ROWS = (1 to 10).map { i =>
      DataRow(new java.math.BigDecimal(i + 1000.0), "USD", "EUR", "2016-01-%02d".format(i + 10))
    }
    val dataRDD = sc.parallelize(INV_DATA_ROWS, 3)
    sqlContext.createDataFrame(dataRDD).registerTempTable(INV_DATA_TABLE)
    setOption(PARAM_ALLOW_INVERSE, "false")
    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
    val ex = intercept[SparkException] {
      sqlContext.sql(s"SELECT $FUNCTION_NAME(amount, from, to, date) FROM $INV_DATA_TABLE")
        .collect()
    }
    assert(ex.getCause.isInstanceOf[ConversionRateNotFoundException])
    setOption(PARAM_ALLOW_INVERSE, "true")
    sqlContext.sql(s"SELECT $FUNCTION_NAME(amount, from, to, date) FROM $INV_DATA_TABLE")
      .collect()
  }

  test("error_handling option") {
    val INV_DATA_TABLE = "inv_data_table"
    val INV_DATA_ROWS = (1 to 10).map { i =>
      DataRow(new java.math.BigDecimal(i + 1000.0), "USD", "EUR", "2016-01-%02d".format(i + 10))
    }
    val dataRDD = sc.parallelize(INV_DATA_ROWS, PARALLELISM)
    sqlContext.createDataFrame(dataRDD).registerTempTable(INV_DATA_TABLE)
    setOption(PARAM_ALLOW_INVERSE, "false")

    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_FAIL)
    val ex = intercept[SparkException] {
      sqlContext.sql(s"SELECT $FUNCTION_NAME(amount, from, to, date) FROM $INV_DATA_TABLE")
        .collect()
    }

    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_KEEP)
    sqlContext.sql(s"SELECT $FUNCTION_NAME(amount, from, to, date) FROM $INV_DATA_TABLE")
      .collect().zip(INV_DATA_ROWS).foreach { case (row, dataRow) =>
      assertComparesEqual(row.getDecimal(0))(dataRow.amount)
    }

    setOption(PARAM_ERROR_HANDLING, ERROR_HANDLING_NULL)
    sqlContext.sql(s"SELECT $FUNCTION_NAME(amount, from, to, date) FROM $INV_DATA_TABLE")
      .collect().foreach { row =>
      assert(row.get(0) == null)
    }
  }

  test("test triple conversion call") {
    val rows = sqlContext.sql(
      s"""SELECT $FUNCTION_NAME(amount, from, to, date),
         |       $FUNCTION_NAME(amount, from, to, date),
         |       $FUNCTION_NAME(amount, from, to, date)
         |FROM   $DATA_TABLE""".stripMargin)
      .collect()
    rows.map(_.getDecimal(0)).zip(EXPECTED_RESULT).foreach {
      case (converted, expected) => assertComparesEqual(converted)(expected)
    }
    rows.map { row => Seq(row.getDecimal(0), row.getDecimal(1), row.getDecimal(2)) }
      .foreach { cols =>
        val value = cols.head
        cols.foreach { assertComparesEqual(_)(value) }
      }
  }

  test("smoke test for DOUBLE usage does not fail (no further guarantees given)") {
    setOption(PARAM_SOURCE_TABLE_NAME, RATES_TABLE_DOUBLE)
    sqlContext.sql(QUERY_FROM_DOUBLE).collect()
  }
}
