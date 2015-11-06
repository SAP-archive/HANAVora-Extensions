package org.apache.spark.sql.catalyst.expressions

import java.sql

case class StringRow(name: String)

case class DoubleRow(name: String, d: Double)

case class DateRow(name: String, d: sql.Date)
