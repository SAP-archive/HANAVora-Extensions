package org.apache.spark.sql.sources

import org.apache.spark.sql.{SapSQLContext, SQLContext, SapParserException}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.RunnableCommand
import org.apache.spark.sql.types.{MetadataBuilder, StringType}
import scala.util.parsing.input.Position


class SapDDLParser(parseQuery: String => LogicalPlan) extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable |
      appendTable |
      dropTable |
      describeTable |
      refreshTable |
      showTables |
      registerAllTables |
      registerTable |
      useStatement

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
  protected val USE = Keyword("USE")

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

  /**
   * Resolves the USE [XYZ ...] statements.
   *
   * A RunnableCommand logging "Statement ignored"
   * is created for each of such statements.
   */
  protected lazy val useStatement: Parser[LogicalPlan] =
    USE ~ rep(".*".r) ^^ {
      case l ~ r => UseStatementCommand(l + " " + r.mkString(" "))
    }

  /*
   * Overridden to appropriately decide which
   * parser error to use in case both parsers (ddl, sql)
   * failed. Now chooses the error of the parser
   * that succeeded most.
   */
  override def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      case vpeDDL: SapParserException =>
        if(exceptionOnError) throw vpeDDL
        // in case ddlparser failed, try sqlparser
        try {
          parseQuery(input)
        }
        catch {
          case vpeSQL: SapParserException =>
            // in case sqlparser also failed,
            // use the exception from the parser
            // that read the most characters
            if(vpeSQL.line > vpeDDL.line) throw vpeSQL
            else if(vpeSQL.line < vpeDDL.line) throw vpeDDL
            else {
              if(vpeSQL.column > vpeDDL.column) throw vpeSQL
              else throw vpeDDL
            }
        }
    }
  }

  /* Overridden to throw a SapParserException
   * instead of as sys.error(failureOrError.toString). This allows
   * to unify the parser exception handling in the
   * upper layers.
   *
   */
  override def parse(input: String): LogicalPlan = {
    // CAUTION:
    // In the overridden method of the super class they use
    // > initLexical
    // to initialize the parser keywords.
    // However, this fails to execute (NoSuchMethod) or even
    // compile with some Spark versions (probably 1.4.0, could
    // not narrow down the issue yet).
    // The following line circumvents the access of the lazy value
    // in the abstract superclass and has been taken from SapSQLParser.
    // It works under Spark 1.4.0 and 1.4.1.
    // In future Spark versions probably the original
    // initialization call can be used again.
    lexical.reserved ++= reservedWords
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        val pos: Position = failureOrError.next.pos
        throw new SapParserException(input, pos.line, pos.column, failureOrError.toString)
    }
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
private[sql] case class ShowDatasourceTablesCommand(classIdentifier: String,
                                                    options: Map[String, String])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq(AttributeReference("tbl_name", StringType,
    nullable = false, new MetadataBuilder()
      .putString("comment", "identifier of the table").build())())

  override def children: Seq[LogicalPlan] = Seq.empty
}


/**
 * Returned for "USE xyz" statements.
 *
 * Currently used to ignore any such statements
 * if "sql.ignore_use_statments=true",
 * else, an exception is thrown.
 *
 * @param input The "USE xyz" input statement
 */
private[sql] case class UseStatementCommand(input: String)
  extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.getConf(SapSQLContext.PROPERTY_IGNORE_USE_STATEMENTS, "false") match {
      case "true" => log.info(s"Ignoring statement:\n$input")
      case _ => throw new SapParserException(input, 1, 1, "USE statement is not supported")
    }
    Nil
  }
}

