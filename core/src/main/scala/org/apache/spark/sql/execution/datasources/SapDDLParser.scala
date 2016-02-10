package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sources.commands._

import scala.util.parsing.input.Position

class SapDDLParser(parseQuery: String => LogicalPlan)
  extends BackportedSapSqlParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
      createViewUsingOrig |
      dropViewUsing |
      createTable |
      createPartitionFunction |
      appendTable |
      dropTable |
      describeTable |
      refreshTable |
      showTables |
      showTablesUsing |
      registerAllTables |
      registerTable |
      describeDatasource |
      useStatement

  protected val APPEND = Keyword("APPEND")
  protected val DROP = Keyword("DROP")
  protected val CASCADE = Keyword("CASCADE")
  // queries the datasource catalog to get all the tables and return them
  // only possible if the appropriate trait is inherited
  protected val SHOW = Keyword("SHOW")
  protected val DSTABLES = Keyword("DATASOURCETABLES")
  protected val REGISTER = Keyword("REGISTER")
  protected val TABLES = Keyword("TABLES")
  protected val IGNORING = Keyword("IGNORING")
  protected val CONFLICTS = Keyword("CONFLICTS")
  protected val USE = Keyword("USE")
  protected val DATASOURCE = Keyword("DATASOURCE")
  protected val PARTITIONED = Keyword("PARTITIONED")
  protected val PARTITION = Keyword("PARTITION")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val PARTITIONS = Keyword("PARTITIONS")

  /* VIEW Keyword */
  protected val VIEW = Keyword("VIEW")
  protected val VIEW_SQL_STRING = "VIEW_SQL"

  /**
    * needed for view parsing.
    */
  lexical.delimiters += "$"

  protected lazy val describeDatasource: Parser[LogicalPlan] =
    DESCRIBE ~> DATASOURCE ~> ident ^^ {
      case tableName =>
        DescribeDatasource(new UnresolvedRelation(Seq(tableName)))
    }

  override protected lazy val createTable: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~
      tableCols.? ~ (PARTITIONED ~> BY ~> functionName ~ colsNames).? ~ (USING ~> className) ~
      (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temp ~ allowExisting ~ tableId ~ columns ~ partitioningFunctionDef ~
        provider ~ opts ~ query =>
        if (temp.isDefined && allowExisting.isDefined) {
          throw new DDLException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        val options = opts.getOrElse(Map.empty[String, String])
        if (query.isDefined) {
          if (columns.isDefined) {
            throw new DDLException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }
          // When IF NOT EXISTS clause appears in the query, the save mode will be ignore.
          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          val queryPlan = parseQuery(query.get)
          CreateTableUsingAsSelect(tableId,
            provider,
            temp.isDefined,
            Array.empty[String],
            mode,
            options,
            queryPlan)
        } else {
          val userSpecifiedSchema = columns.flatMap(fields => Some(StructType(fields)))
          if (partitioningFunctionDef.isDefined) {
            val partitioningFunction = partitioningFunctionDef.get._1
            val partitioningColumns = partitioningFunctionDef.get._2
            CreateTablePartitionedByUsing(
              tableId,
              userSpecifiedSchema,
              provider,
              partitioningFunction,
              partitioningColumns,
              temp.isDefined,
              options,
              allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateTableUsing(
              tableId,
              userSpecifiedSchema,
              provider,
              temp.isDefined,
              options,
              allowExisting.isDefined,
              managedIfNoPath = false)
          }
        }
    }

  /**
   * Resolves the CREATE PARTITIONING FUNCTION statements.
   * The only parameter is the function definition passed by
   * the user,
   */
  protected lazy val createPartitionFunction: Parser[LogicalPlan] =
    (CREATE ~> PARTITION ~> FUNCTION ~> ident) ~ datatypes ~ (AS ~> ident) ~
      (PARTITIONS ~> numericLit).? ~ (USING ~> className) ~
      (OPTIONS ~> options).? ^^ {
      case name ~ types ~ definition ~ partitionsNo ~ provider ~ opts =>
        if (types.isEmpty){
          throw new DDLException(
            "The hashing function argument list cannot be empty.")
        }
        val options = opts.getOrElse(Map.empty[String, String])
        val partitionsNoInt = if (partitionsNo.isDefined) Some(partitionsNo.get.toInt) else None
        CreatePartitioningFunction(options, name, types, definition, partitionsNoInt, provider)
    }

  /**
    * Resolves a CREATE VIEW ... USING statement, in addition is adds the original VIEW SQL string
    * added by the user into the OPTIONS.
    */
  protected lazy val createViewUsingOrig: Parser[LogicalPlan] =
    withConsumedInput(createViewUsing) ^^ {
      case ((name, plan, provider, opts, allowExisting), text) =>
        CreatePersistentViewCommand(name, plan, provider, opts.updated(VIEW_SQL_STRING, text.trim),
          allowExisting)
    }

  /**
    * Resolves a DROP VIEW ... USING statement. For more information about the rationale behind
    * this command please take a look at [[DropPersistentViewRunnableCommand]]
    */
  protected lazy val dropViewUsing: Parser[LogicalPlan] =
    DROP ~> VIEW ~> (IF ~> EXISTS).? ~ (ident <~ ".").? ~ ident ~ (USING ~> className) ~
      (OPTIONS ~> options).? ^^ {
      case allowNotExisting ~ db ~ tbl ~ provider ~ opts =>
        DropPersistentViewCommand(TableIdentifier(tbl, db),
          provider, opts.getOrElse(Map.empty[String, String]), allowNotExisting.isDefined)
    }

  /** Create view parser rule */
  protected lazy val createViewUsing: Parser[(TableIdentifier, LogicalPlan,
    String, Map[String, String], Boolean)] =
    CREATE ~> VIEW ~> (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~ (AS ~> start1) ~
      (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case allowExisting ~ name ~ plan ~ provider ~ opts =>
        (name, plan, provider, opts.getOrElse(Map.empty[String, String]), allowExisting.isDefined)
    }

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
    DROP ~> TABLE ~> (IF ~> EXISTS).? ~ (ident <~ ".").? ~ ident ~ CASCADE.? ^^ {
      case allowNotExisting ~ db ~ tbl ~ cascade =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            Seq(dbName, tbl)
          case None =>
            Seq(tbl)
        }
        DropCommand(allowNotExisting.isDefined,
          UnresolvedRelation(tblIdentifier, None), cascade.isDefined)
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
    * Resolves the command: ''SHOW TABLES USING ... OPTIONS''
    */
  protected lazy val showTablesUsing: Parser[LogicalPlan] =
    SHOW ~> TABLES ~> (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case classId ~ opts =>
        val options = opts.getOrElse(Map.empty[String, String])
        ShowTablesUsingCommand(classId, options)
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

  protected lazy val datatypes: Parser[Seq[DataType]] = "(" ~> repsep(primitiveType, ",") <~ ")"

  protected lazy val colsNames: Parser[Seq[String]] = "(" ~> repsep(ident, ",") <~ ")"

  /** Parses the content of OPTIONS and puts the result in a case insensitive map */
  override protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ {
      case s: Seq[(String, String)] => new CaseInsensitiveMap(s.toMap)
    }

  /** Partitioning function identifier */
  protected lazy val functionName: Parser[String] = ident

  /**
    * Overridden to appropriately decide which
    * parser error to use in case both parsers (ddl, sql)
    * failed. Now chooses the error of the parser
    * that succeeded most.
    *
    * @param input the input string.
    * @param exceptionOnError if set to true, the parser will throw an exception without falling
    *                         back to the DML parser. If set to false, then the parser will only
    *                         throw without falling back to DML parser if a [[DDLException]] occurs.
    *
   */
  override def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      // DDLException indicates that although the statement if valid there is still a problem with
      // it (e.g. a CREATE which have IF NOT EXISTS and TEMPORARY). Therefor we must throw
      // immediately because it does not make sense to try to parse it with the DML parser.
      case ddlException: DDLException =>
        throw ddlException
      case vpeDDL: SapParserException =>
        if (exceptionOnError) throw vpeDDL
        // in case ddlparser failed, try sqlparser
        try {
          parseQuery(input)
        }
        catch {
          case vpeSQL: SapParserException =>
            // in case sqlparser also failed,
            // use the exception from the parser
            // that read the most characters
            (vpeSQL.line compare vpeDDL.line).signum match {
              case 1 => throw vpeSQL
              case -1 => throw vpeDDL
              case 0 =>
                if (vpeSQL.column > vpeDDL.column) {
                  throw vpeSQL
                } else {
                  throw vpeDDL
                }
            }
        }
    }
  }

  override def parse(input: String): LogicalPlan = synchronized {
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        // Now the native scala parser error is reformatted
        // to be non-misleading. An idea is to allow the user
        // to set the error message type in the future.
        val pos: Position = failureOrError.next.pos
        throw new SapParserException(input, pos.line, pos.column, failureOrError.toString)
    }
  }

  /**
    * Helper method that keeps in the input of a parsed input using parser 'p'.
    *
    * @param p The original parser.
    * @tparam U The output type of 'p'
    * @return a tuple containing the original parsed input along with the input [[String]].
    */
  def withConsumedInput [U](p: => Parser[U]): Parser[(U, String)] = new Parser[(U, String)] {
    def apply(in: Input) = p(in) match {
      case Success(result, next) =>
        val parsedString = in.source.subSequence(in.offset, next.offset).toString
        Success(result -> parsedString, next)
      case other: NoSuccess => other
    }
  }
}
