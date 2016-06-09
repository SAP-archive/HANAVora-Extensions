package org.apache.spark.sql.currency


package object erp {

  val CONF_PREFIX = "spark.sql.currency.erp."

  val PARAM_TABLE_PREFIX = "table_prefix"
  val PARAM_DO_UPDATE = "do_update"
  val PARAM_ACCURACY = "accuracy"
  val PARAM_DATE_FORMAT = "date_format"
  val PARAM_ERROR_HANDLING = "error_handling"
  val PARAM_LOOKUP = "lookup"
  val PARAM_STEPS = "steps"

  // The name of each of the 5 conversion tables can be specified by:
  // "spark.sql.currency.erp.tcurX = NAME"
  // Default names are the table keys (upper cased)
  val PARAM_TABLE_NAMES = List("tcurx", "tcurv", "tcurf", "tcurr", "tcurn")
  val DEFAULT_TABLES_MAP =
    PARAM_TABLE_NAMES.zip(PARAM_TABLE_NAMES.map(_.toUpperCase)).toMap

  val DO_UPDATE_FALSE = "false"
  val DO_UPDATE_TRUE = "true"
  val ERROR_HANDLING_FAIL = "fail_on_error"
  val ERROR_HANDLING_NULL = "set_to_null"
  val ERROR_HANDLING_KEEP = "keep_unconverted"

  val DEFAULT_TABLE_PREFIX = ""
  val DEFAULT_ACCURACY = "compatibility"
  val DEFAULT_DATE_FORMAT = "auto_detect"
  val DEFAULT_ERROR_HANDLING = "fail on error"
  val DEFAULT_LOOKUP = "regular"
  val DEFAULT_STEPS = "shift, convert"

  val DEFAULT_OPTIONS_MAP =
    Map(CONF_PREFIX + PARAM_TABLE_PREFIX -> DEFAULT_TABLE_PREFIX,
        CONF_PREFIX + PARAM_ACCURACY -> DEFAULT_ACCURACY,
        CONF_PREFIX + PARAM_DATE_FORMAT -> DEFAULT_DATE_FORMAT,
        CONF_PREFIX + PARAM_ERROR_HANDLING -> DEFAULT_ERROR_HANDLING,
        CONF_PREFIX + PARAM_LOOKUP -> DEFAULT_LOOKUP,
        CONF_PREFIX + PARAM_STEPS -> DEFAULT_STEPS,
        CONF_PREFIX + PARAM_DO_UPDATE -> DO_UPDATE_FALSE) ++
      DEFAULT_TABLES_MAP.map {
        case (key, value) => (CONF_PREFIX + key, value)
      }
}
