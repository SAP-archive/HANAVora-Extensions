package org.apache.spark.sql.catalyst

/**
  * In Spark 1.6 the TableIdentifier is heavily used. However, its interface is private and
  * for the outside the database.tableName notation is used as a sequence of Strings split by "."s
  * This implicit enhances the TableIdentifier by .toSeq
  */
object TableIdentifierUtils {
  implicit class RichTableIdentifier(tId: TableIdentifier) {
    def toSeq: Seq[String] = tId.database.toSeq :+ tId.table
  }
}
