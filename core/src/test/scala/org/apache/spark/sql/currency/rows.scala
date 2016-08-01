package org.apache.spark.sql.currency

case class DataRow(amount: java.math.BigDecimal, from: String, to: String, date: String)

case class DataRowUsingDouble(amount: Double, from: String, to: String, date: String)

case class ERPDataRow(
    client: String,
    method: String,
    amount: java.math.BigDecimal,
    from: String,
    to: String,
    date: String)

case class RateRow(from: String, to: String, date: String, rate: java.math.BigDecimal)

case class RateRowUsingDouble(from: String, to: String, date: String, rate: Double)
