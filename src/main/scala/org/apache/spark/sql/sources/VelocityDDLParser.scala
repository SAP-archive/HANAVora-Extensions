package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}

class VelocityDDLParser(parseQuery: String => LogicalPlan) extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | appendTable | describeTable | refreshTable

  protected val APPEND = Keyword("APPEND")

  /**
   * Resolves the APPEND TABLE statements:
   *
   * `APPEND TABLE tableName
   * OPTIONS (hosts "host1,host2",
   *          paths "path1/to/file/file1.csv,path2/to/file/file2.csv",
   *          [eagerLoad "false"])`

   * The eagerLoad parameter is set to "true" by default.
   */
  protected lazy val appendTable: Parser[LogicalPlan] =
    APPEND ~> TABLE ~> (ident <~ ".").? ~ ident ~ (OPTIONS ~> options) ^^ {
      case db ~ tbl ~ opts =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        AppendCommand(UnresolvedRelation(tblIdentifier, None), opts)
    }

}

/**
 * Returned for the "APPEND  [dbName.]tableName" command.
 * @param table The table where the file is going to be appended
 * @param options The options map with the append configuration
 */
private[sql] case class AppendCommand(table: LogicalPlan,
                                      options: Map[String, String]) extends Command
