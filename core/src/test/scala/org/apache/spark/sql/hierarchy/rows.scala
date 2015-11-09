package org.apache.spark.sql.hierarchy

case class ComponentRow(extraField: String, name: String, pred: Long, succ: Long, ord: Long)

case class EmployeeRow(name: String, pred: Option[Long], succ: Long, ord: Int)

case class AnimalRow(name: String, pred: Option[Long], succ: Long, ord: Long)

case class AddressRow(name: String, address: String)

case class SensorRow(sensor: String, par: String, name: String)

case class PartialResult(path: Seq[Long], pk: Long)
