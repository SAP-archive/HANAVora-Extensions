package org.apache.spark.sql.currency


package object basic {

  val CONF_PREFIX = "spark.sql.currency.basic."

  val PARAM_SOURCE_TABLE_NAME = "table"
  val PARAM_ALLOW_INVERSE = "allow_inverse"
  val PARAM_DO_UPDATE = "do_update"
  val PARAM_ERROR_HANDLING = "error_handling"

  val DO_UPDATE_FALSE = "false"
  // These names come from HANA convert_currency.
  // Used here to be a bit API-compatible.
  val ERROR_HANDLING_NULL = "set_to_null"
  val ERROR_HANDLING_KEEP = "keep_unconverted"
  val ERROR_HANDLING_FAIL = "fail_on_error"

  val DEFAULT_ALLOW_INVERSE = "false"
  val DEFAULT_ERROR_HANDLING = ERROR_HANDLING_NULL
  val DEFAULT_RATES_TABLE = "RATES"

  val DEFAULT_OPTIONS_MAP =
    Map(CONF_PREFIX + PARAM_SOURCE_TABLE_NAME -> DEFAULT_RATES_TABLE,
        CONF_PREFIX + PARAM_ERROR_HANDLING -> DEFAULT_ERROR_HANDLING,
        CONF_PREFIX + PARAM_ALLOW_INVERSE -> DEFAULT_ALLOW_INVERSE,
        CONF_PREFIX + PARAM_DO_UPDATE -> DO_UPDATE_FALSE)
}
