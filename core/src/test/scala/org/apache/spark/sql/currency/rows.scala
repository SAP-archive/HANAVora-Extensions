package org.apache.spark.sql.currency

case class DataRow(amount: Double, from: String, to: String, date: String)

case class ERPDataRow(
    client: String,
    method: String,
    amount: Double,
    from: String,
    to: String,
    date: String)

case class RateRow(from: String, to: String, date: String, rate: Double)
