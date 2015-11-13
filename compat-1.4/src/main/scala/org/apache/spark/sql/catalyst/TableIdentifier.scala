package org.apache.spark.sql.catalyst

/**
  * Identifies a `table` in `database`.  If `database` is not defined, the current database is used.
  *
  * Backported form Spark 1.5.
  */
private[sql] case class TableIdentifier(table: String, database: Option[String] = None) {
  def withDatabase(database: String): TableIdentifier = this.copy(database = Some(database))

  def toSeq: Seq[String] = database.toSeq :+ table

  override def toString: String = quotedString

  def quotedString: String = toSeq.map("`" + _ + "`").mkString(".")

  def unquotedString: String = toSeq.mkString(".")
}
