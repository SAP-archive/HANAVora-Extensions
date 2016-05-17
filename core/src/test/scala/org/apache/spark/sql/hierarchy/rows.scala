package org.apache.spark.sql.hierarchy

case class ComponentRow(extraField: String, name: String, pred: Long, succ: Long, ord: Long)

case class EmployeeRow(name: String, pred: Option[Long], succ: Long, ord: Int)

case class LevelEmployeeRow(col1: String, col2: String, col3: String, col4: String)

case class NumericLevelRow(col1: Int, col2: Int, col3: Int)

case class AnimalRow(name: String, pred: Option[Long], succ: Long, ord: Long)

case class LevelAnimalRow(col1: String, col2: String, col3: String, ord: Long)

case class AddressRow(name: String, address: String)

case class SensorRow(sensor: String, par: String, name: String)

case class PartialResult(path: Seq[Long], pk: Long)
