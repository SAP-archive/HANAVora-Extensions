package org.apache.spark.sql

import org.apache.spark.sql.util.ParseErrorFormatter

class SapParserException(
    val input: String,
    val line: Int,
    val column: Int,
    val origErrorMessage: String)
    // TODO (non-critical): Move
    // ParseErrorFormatter.nonmisleadingParseErrorMessage
    // to sqlContext.sql method by catching this
    // exception and evaluating its status.
  extends RuntimeException(
    ParseErrorFormatter.nonmisleadingParseErrorMessage(
      input, line, column))
