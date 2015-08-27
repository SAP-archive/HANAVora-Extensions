package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.types.{MetadataBuilder, StringType}

class VelocityDDLParser(parseQuery: String => LogicalPlan) extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable |
      appendTable |
      dropTable |
      describeTable |
      refreshTable |
      showTables |
      registerAllTables |
      registerTable

  protected val APPEND = Keyword("APPEND")
  protected val DROP = Keyword("DROP")
  // queries the datasource catalog to get all the tables and return them
  // only possible if the appropriate trait is inherited
  protected val SHOW = Keyword("SHOW")
  protected val DSTABLES = Keyword("DATASOURCETABLES")
  protected val REGISTER = Keyword("REGISTER")
  protected val ALL = Keyword("ALL")
  protected val TABLES = Keyword("TABLES")
  protected val IGNORING = Keyword("IGNORING")
  protected val CONFLICTS = Keyword("CONFLICTS")

  /**
   * Resolves the REGISTER ALL TABLES statements:
   * REGISTER ALL TABLES USING provider.name
   * OPTIONS(optiona "option a",optionb "option b")
   * IGNORING CONFLICTS
   *
   * The IGNORING CONFLICTS command is used to avoid
   * registration of existing tables in spark catalog.
   */
  protected lazy val registerAllTables: Parser[LogicalPlan] =
    REGISTER ~> ALL ~> TABLES ~> (USING ~> className) ~
      (OPTIONS ~> options).? ~ (IGNORING ~> CONFLICTS).? ^^ {
      case provider ~ opts ~ ignoreConflicts =>
        RegisterAllTablesUsing(
          provider = provider,
          options = opts.getOrElse(Map.empty[String, String]),
          ignoreConflicts.isDefined
        )
    }

  /**
   * Resolves the REGISTER TABLE statement:
   * REGISTER TABLE tableName USING provider.name
   * OPTIONS(optiona "option a",optionb "option b")
   * IGNORING CONFLICTS
   */
  protected lazy val registerTable: Parser[LogicalPlan] =
    REGISTER ~> TABLE ~> ident ~ (USING ~> className) ~
      (OPTIONS ~> options).? ~ (IGNORING ~> CONFLICTS).? ^^ {
      case tbl ~ provider ~ opts ~ ignoreConflicts =>
        RegisterTableUsing(
          tableName = tbl,
          provider = provider,
          options = opts.getOrElse(Map.empty[String, String]),
          ignoreConflicts.isDefined
        )
    }

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

  /**
   * Resolves the DROP TABLE statements:
   *
   * `DROP TABLE tableName`
   */
  protected lazy val dropTable: Parser[LogicalPlan] =
    DROP ~> TABLE ~> (ident <~ ".").? ~ ident ^^ {
      case db ~ tbl =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        DropCommand(UnresolvedRelation(tblIdentifier, None))
    }

  /**
   * Resolves the SHOW DATASOURCETABLES statements:
   *
   * `SHOW VTABLES`
   */
  protected lazy val showTables: Parser[LogicalPlan] =
    SHOW ~> DSTABLES ~> (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case classId ~ opts =>
        val options = opts.getOrElse(Map.empty[String, String])
        ShowDatasourceTablesCommand(classId, options)
    }

}

private[sql] case class RegisterAllTablesUsing(
                                                provider: String,
                                                options: Map[String, String],
                                                ignoreConflicts: Boolean
                                                ) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class RegisterTableUsing(
                                          tableName: String,
                                          provider: String,
                                          options: Map[String, String],
                                          ignoreConflict: Boolean
                                            ) extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
 * Returned for the "APPEND TABLE [dbName.]tableName" command.
 * @param table The table where the file is going to be appended
 * @param options The options map with the append configuration
 */
private[sql] case class AppendCommand(table: LogicalPlan,
                                      options: Map[String, String])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
 * Returned for the "DROP TABLE [dbName.]tableName" command.
 * @param table The table to be dropped
 */
private[sql] case class DropCommand(table: LogicalPlan) extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty
}

/**
 * Returned for the "SHOW DATASOURCETABLES" command.
 */
private[sql] case class ShowDatasourceTablesCommand(classIdentifier : String,
                                                    options: Map[String, String])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq(AttributeReference("tbl_name", StringType,
    nullable = false, new MetadataBuilder()
      .putString("comment", "identifier of the table").build())())

  override def children: Seq[LogicalPlan] = Seq.empty
}
