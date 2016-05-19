package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, AnnotatedAttribute, AnnotationFilter, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Cube => _, _}
import org.apache.spark.sql.sources.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{AnnotationParsingRules, SapParserException}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation}
import org.apache.spark.sql.sources.commands._

import scala.util.parsing.input.Position
import scala.reflect._
import org.apache.spark.sql.util.CollectionUtils._

// scalastyle: off file.size.limit

// note(mathis): this is a temporary solution / hack until
//               parsing new engine DDL is included in the
//               standard DDL parser
class SapDDLParser(parseQuery: String => LogicalPlan)
  extends InternalSapDDLParser(parseQuery) {

  protected lazy val usingWithClassName: Parser[String] = {
    USING ~> className ^^ {
      case provider =>
        provider
    }
  }
  /**
    * Overrides standard DDL parsing, by checking for 'USING com.sap.spark.engines' to emit
    * a [[RawDDLCommand]] or the standard DDL plan.
    *
    * This hacky hack way will be removed, once proper DDL parsing is in place.
    *
    * @param input The string to parse for DDL
    * @return A RawDDLCommand if this query contains 'USING com.sap.spark.engines', the standard
    *         plan otherwise
    */
  override def parse(input: String): LogicalPlan = {
    // check for USING com.sap.spark.engines
    val idx = input.toUpperCase.lastIndexOf(USING.normalize.toUpperCase())
    if(idx == -1) {
      super.parse(input)
    } else {
      val parts = input.splitAt(idx)
      initLexical
      try {
        phrase(usingWithClassName)(new lexical.Scanner(parts._2)) match {
          case Success(provider, _) =>
            if(provider == RawDDLCommand.provider) {
              RawDDLCommand(ddlStatement = parts._1.trim)
            } else {
              super.parse(input)
            }
          case failureOrError =>
            super.parse(input) // parse on InternalSapDDLParser
        }
      }
    }
  }
}

// note(mathis): temporarily renamed from SapDDLParser to InternalSapDDLParser
class InternalSapDDLParser(parseQuery: String => LogicalPlan)
  extends BackportedSapSqlParser(parseQuery)
  with AnnotationParsingRules {

  override protected lazy val ddl: Parser[LogicalPlan] =
      createViewUsingOrig |
      dropViewUsing |
      describeTableUsing |
      createTable |
      createHashPartitionFunction |
      createRangeSplitPartitionFunction |
      createRangeIntervalPartitionFunction |
      dropPartitionFunction |
      appendTable |
      dropTable |
      describeTable |
      refreshTable |
      showTables |
      showTablesUsing |
      showPartitionFunctionsUsing |
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
  protected val DEEP = Keyword("DEEP")
  protected val PARTITIONED = Keyword("PARTITIONED")
  protected val PARTITION = Keyword("PARTITION")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val FUNCTIONS = Keyword("FUNCTIONS")
  protected val HASH = Keyword("HASH")
  protected val RANGE = Keyword("RANGE")
  protected val PARTITIONS = Keyword("PARTITIONS")
  protected val SPLITTERS = Keyword("SPLITTERS")
  protected val CLOSED = Keyword("CLOSED")
  protected val STRIDE = Keyword("STRIDE")
  protected val PARTS = Keyword("PARTS")

  /* VIEW Keyword */
  protected val VIEW = Keyword("VIEW")
  protected val VIEW_SQL_STRING = "VIEW_SQL"
  protected val DIMENSION = Keyword("DIMENSION")
  protected val CUBE = Keyword("CUBE")

  /* CREATE TABLE CONSTANTS */
  protected val TABLE_DDL_STATEMENT = "TABLE_DDL"

  /**
    * needed for view parsing.
    */
  lexical.delimiters += "$"

  protected lazy val describeDatasource: Parser[LogicalPlan] =
    DEEP ~> DESCRIBE ~> ident ^^ {
      case tableName =>
        UnresolvedDeepDescribe(new UnresolvedRelation(TableIdentifier(tableName)))
    }

  override protected lazy val createTable: Parser[LogicalPlan] =
    withConsumedInput((CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~
      tableCols.? ~ (PARTITIONED ~> BY ~> functionName ~ colsNames).? ~ (USING ~> className) ~
      (OPTIONS ~> options).? ~ (AS ~> restInput).?) ^^ {
      case (temp ~ allowExisting ~ tableId ~ columns ~ partitioningFunctionDef ~
        provider ~ opts ~ query, ddlStatement) =>
        if (temp.isDefined && allowExisting.isDefined) {
          throw new DDLException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        val options =
          opts.getOrElse(CaseInsensitiveMap.empty[String])
              .putIfAbsent(TABLE_DDL_STATEMENT, ddlStatement)
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
          /**
            * This checks for the deprecated path(s) option, should be removed in the next
            * version
            */
          val sapVoraProviderPackages: Set[String] =
            Set("com.sap.spark.vora", "com.sap.spark.vora.DefaultSource")
          val sapHanaProviderPackages: Set[String] =
            Set("com.sap.spark.hana", "com.sap.spark.hana.DefaultSource")

          val modifiedOptions: Map[String, String] =
            (sapVoraProviderPackages.contains(provider),
              options.keySet.contains("paths"),
              sapHanaProviderPackages.contains(provider),
              options.keySet.contains("path")) match {
            // paths for vora datasource set
            case (true, true, false, _) =>
              logWarning(s"Usage of 'paths' is a deprecated option for datasource '$provider' " +
                s"use 'files' instead")
              // replace paths
              options - ("paths") + (("files" -> options.get("paths").get))
            // path for hana datasource set
            case (false, _, true, true) =>
              logWarning(s"Usage of 'path' is a deprecated option for datasource '$provider' " +
                s"use 'tablepath' instead")
              // replace paths
              options - ("path") + (("tablepath" -> options.get("path").get))
            // leave untouched by default
            case _ => options
          }

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
              modifiedOptions,
              allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateTableUsing(
              tableId,
              userSpecifiedSchema,
              provider,
              temp.isDefined,
              modifiedOptions,
              allowExisting.isDefined,
              managedIfNoPath = false)
          }
        }
    }

  /**
    * Resolves the CREATE PARTITIONING FUNCTION statements for hash
    * partitioning functions.
    * The only parameter is the function definition passed by
    * the user,
    */
  protected lazy val createHashPartitionFunction: Parser[LogicalPlan] =
    CREATE ~> PARTITION ~> FUNCTION ~> ident ~ datatypes ~ (AS ~> HASH) ~
      (PARTITIONS ~> numericLit).? ~ (USING ~> className) ~
      (OPTIONS ~> options).? ^^ {
      case name ~ types ~ definition ~ partitionsNo ~ provider ~ opts =>
        if (types.isEmpty){
          throw new DDLException(
            "The hashing function argument list cannot be empty.")
        }
        val options = opts.getOrElse(Map.empty[String, String])
        val partitionsNoInt = if (partitionsNo.isDefined) Some(partitionsNo.get.toInt) else None
        CreateHashPartitioningFunction(options, name, provider, types, partitionsNoInt)
    }

  /**
    * Resolves the CREATE PARTITIONING FUNCTION statements for range
    * partitioning functions defined by splitters.
    * The only parameter is the function definition passed by
    * the user,
    */
  protected lazy val createRangeSplitPartitionFunction: Parser[LogicalPlan] =
    CREATE ~> PARTITION ~> FUNCTION ~> ident ~ datatypes ~ (AS ~> RANGE) ~
      (SPLITTERS ~> (RIGHT ~> CLOSED).?) ~ numbers ~
      (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case name ~ types ~ definition ~ closed ~ splitters ~ provider ~ opts =>
        if (types.size != 1) {
          if (types.isEmpty) {
            throw new DDLException(
              "The range function argument list cannot be empty.")
          } else {
            throw new DDLException(
              "The range functions cannot have more than one argument.")
          }
        }
        val rightClosed = closed.isDefined
        val splittersConv = if (splitters.isEmpty){
          throw new DDLException(
            "The range function splitters cannot be empty.")
        } else {
          splitters.map(_.toInt)
        }
        val options = opts.getOrElse(Map.empty[String, String])
        CreateRangeSplittersPartitioningFunction(options, name, provider, types.head,
          splittersConv, rightClosed)
    }

  /**
    * Resolves the CREATE PARTITIONING FUNCTION statements for range
    * partitioning functions defined by an interval.
    * The only parameter is the function definition passed by
    * the user,
    */
  protected lazy val createRangeIntervalPartitionFunction: Parser[LogicalPlan] =
    CREATE ~> PARTITION ~> FUNCTION ~> ident ~ datatypes ~ (AS ~> RANGE) ~
      (START ~> numericLit) ~ (END ~> numericLit) ~ (STRIDE | PARTS) ~ numericLit ~
      (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case name ~ types ~ definition ~ start ~ end ~ strideType ~ strideValue ~ provider ~ opts =>
        if (types.size != 1) {
          if (types.isEmpty) {
            throw new DDLException(
              "The range function argument list cannot be empty.")
          } else {
            throw new DDLException(
              "The range functions cannot have more than one argument.")
          }
        }
        val strideParts = if (strideType.compareToIgnoreCase(STRIDE.str) == 0) {
          Left(strideValue.toInt)
        } else {
          Right(strideValue.toInt)
        }
        val options = opts.getOrElse(Map.empty[String, String])
        CreateRangeIntervalPartitioningFunction(options, name, provider, types.head,
          start.toInt, end.toInt, strideParts)
    }

  /**
    * Resolves a CREATE VIEW ... USING statement, in addition is adds the original VIEW SQL string
    * added by the user into the OPTIONS.
    */
  protected lazy val createViewUsingOrig: Parser[LogicalPlan] =
    withConsumedInput(createViewUsing) ^^ {
      case ((name, plan, provider, opts, allowExisting, kind), text) =>
        val view = kind match {
          case Dimension => PersistedDimensionView(plan)
          case Plain => PersistedView(plan)
          case Cube => PersistedCubeView(plan)
        }
        CreatePersistentViewCommand[AbstractView with Persisted](
          view, name, provider,
          opts.updated(VIEW_SQL_STRING, text.trim), allowExisting)(
          view.tag.asInstanceOf[ClassTag[AbstractView with Persisted]])
      }

  /**
    * Resolves a DROP VIEW ... USING statement. For more information about the rationale behind
    * this command please take a look at [[DropPersistentViewCommand]]
    */
  protected lazy val dropViewUsing: Parser[LogicalPlan] =
    DROP ~> viewKind ~ (IF ~> EXISTS).? ~ tableIdentifier ~ (USING ~> className) ~
      (OPTIONS ~> options).? ^^ {
      case kind ~ allowNotExisting ~ identifier ~ provider ~ opts =>
        DropPersistentViewCommand(identifier, provider,
          opts.getOrElse(Map.empty[String, String]), allowNotExisting.isDefined)(kind.relatedTag)
    }

  protected lazy val viewKind: Parser[ViewKind] = (DIMENSION | CUBE).? <~ VIEW ^^ {
    case ViewKind(kind) => kind
  }

  /** Create view parser rule */
  protected lazy val createViewUsing: Parser[(TableIdentifier, LogicalPlan,
    String, Map[String, String], Boolean, ViewKind)] =
    (CREATE ~> viewKind) ~ (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~ (AS ~> start1) ~
      (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case kind ~ allowExisting ~ name ~ plan ~ provider ~ opts =>
        (name, plan, provider, opts.getOrElse(Map.empty[String, String]), allowExisting.isDefined,
          kind)
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
    *
    * The eagerLoad parameter is set to "true" by default.
   */
  protected lazy val appendTable: Parser[LogicalPlan] =
    APPEND ~> TABLE ~> (ident <~ ".").? ~ ident ~ (OPTIONS ~> options) ^^ {
      case db ~ tbl ~ opts =>
        val tblIdentifier = db match {
          case Some(dbName) =>
            TableIdentifier(dbName, Some(tbl))
          case None =>
            TableIdentifier(tbl)
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
        val tableIdent = TableIdentifier(tbl, db)
        DropCommand(allowNotExisting.isDefined, tableIdent, cascade.isDefined)
    }

  /**
   * Resolves the DROP PARTITIONING FUNCTION statement.
   * The only parameter is the function definition passed by
   * the user,
   */
  protected lazy val dropPartitionFunction: Parser[LogicalPlan] =
    DROP ~> PARTITION ~> FUNCTION ~> (IF ~> EXISTS).? ~ ident ~
      (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case allowNotExisting ~ name ~ provider ~ opts =>
        val options = opts.getOrElse(Map.empty[String, String])
        DropPartitioningFunction(options, name, provider, allowNotExisting.isDefined)
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

  protected lazy val showPartitionFunctionsUsing: Parser[LogicalPlan] = {
    SHOW ~> PARTITION ~> FUNCTIONS ~> (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case classId ~ opts =>
        val options = opts.getOrElse(Map.empty)
        ShowPartitionFunctionsUsingCommand(classId, options)
    }
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

  protected lazy val numbers: Parser[Seq[String]] = "(" ~> repsep(numericLit, ",") <~ ")"

  /** Parses the content of OPTIONS and puts the result in a case insensitive map */
  override protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ {
      case s: Seq[(String, String)] => CaseInsensitiveMap(s.toMap)
    }

  /** Partitioning function identifier */
  protected lazy val functionName: Parser[String] = ident

  /**
    * Parser of the DESCRIBE TABLE ... USING statement.
    */
  protected lazy val describeTableUsing: Parser[LogicalPlan] =
    DESCRIBE ~> TABLE ~> tableIdentifier ~ (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case tableIdent ~ provider ~ opts =>
        DescribeTableUsingCommand(tableIdent, provider, opts.getOrElse(Map.empty))
    }

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
   * This is copied from [[projection]] but extended to allow annotations
   * on the attributes.
   */
  override protected lazy val projection: Parser[Expression] =
    (expression ~ (AS ~> ident) ~ metadata ^^ {
      case e ~ a ~ k => AnnotatedAttribute(Alias(e, a)())(k)
    }
      | expression ~ metadataFilter ^^ {
      case e ~ f => AnnotationFilter(e)(f)
    }
      | rep1sep(ident, ".") ~ metadata ^^ {
      case e ~ k =>
        AnnotatedAttribute(Alias(UnresolvedAttribute(e), e.last)())(k)
    }
      | expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }
      )

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

  /**
   * Overridden to allow the user to add annotations on the table columns.
   */
  override lazy val tableCols: Parser[Seq[StructField]] = "(" ~> repsep(annotatedCol, ",") <~ ")"

  protected lazy val annotatedCol: Parser[StructField] =
    (ident ~ metadata ~ dataType ^^ {
      case columnName ~ md ~ typ =>
        StructField(columnName, typ, nullable = true, metadata = toTableMetadata(md))
    }
    |ident ~ dataType ~ (COMMENT ~> stringLit).?  ^^ { case columnName ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(COMMENT.str.toLowerCase, comment).build()
        case None => Metadata.empty
      }

      StructField(columnName, typ, nullable = true, meta)
    })
}
// scalastyle: on
