package org.apache.spark.sql.parser

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedExtractValue, UnresolvedFunction, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.tablefunctions.UnresolvedTableFunction
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Complete, Count, HyperLogLogPlusPlus}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.commands._
import org.apache.spark.sql.sources.sql.ViewKind
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils.CaseInsensitiveMap

import scala.util.parsing.input.Position

/**
 * SQL parser based on [[org.apache.spark.sql.catalyst.SqlParser]] with
 * extended syntax and fixes.
 *
 * This parser covers only SELECT and CREATE [TEMPORARY] VIEW statements.
 * For DML statements see [[SapDDLParser]].
 */
private[sql] object SapDQLParser
  extends AbstractSparkSQLParser
  with DataTypeParser
  with LiteralParser
  with AnnotationParser
  with WithConsumedInputParser {

  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val ALL = Keyword("ALL")
  protected val AND = Keyword("AND")
  protected val APPROXIMATE = Keyword("APPROXIMATE")
  protected val AS = Keyword("AS")
  protected val ASC = Keyword("ASC")
  protected val BETWEEN = Keyword("BETWEEN")
  protected val BY = Keyword("BY")
  protected val CASE = Keyword("CASE")
  protected val CAST = Keyword("CAST")
  protected val DESC = Keyword("DESC")
  protected val DISTINCT = Keyword("DISTINCT")
  protected val ELSE = Keyword("ELSE")
  protected val END = Keyword("END")
  protected val EXCEPT = Keyword("EXCEPT")
  protected val FROM = Keyword("FROM")
  protected val FULL = Keyword("FULL")
  protected val GROUP = Keyword("GROUP")
  protected val HAVING = Keyword("HAVING")
  protected val IN = Keyword("IN")
  protected val INNER = Keyword("INNER")
  protected val INSERT = Keyword("INSERT")
  protected val INTERSECT = Keyword("INTERSECT")
  protected val INTO = Keyword("INTO")
  protected val IS = Keyword("IS")
  protected val JOIN = Keyword("JOIN")
  protected val LEFT = Keyword("LEFT")
  protected val LIKE = Keyword("LIKE")
  protected val LIMIT = Keyword("LIMIT")
  protected val NOT = Keyword("NOT")
  protected val ON = Keyword("ON")
  protected val OR = Keyword("OR")
  protected val ORDER = Keyword("ORDER")
  protected val SORT = Keyword("SORT")
  protected val OUTER = Keyword("OUTER")
  protected val OVERWRITE = Keyword("OVERWRITE")
  protected val REGEXP = Keyword("REGEXP")
  protected val RIGHT = Keyword("RIGHT")
  protected val RLIKE = Keyword("RLIKE")
  protected val SELECT = Keyword("SELECT")
  protected val SEMI = Keyword("SEMI")
  protected val TABLE = Keyword("TABLE")
  protected val THEN = Keyword("THEN")
  protected val UNION = Keyword("UNION")
  protected val WHEN = Keyword("WHEN")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = Keyword("WITH")

  /* Hierarchies keywords */
  protected val HIERARCHY = Keyword("HIERARCHY")
  protected val USING = Keyword("USING")
  protected val PRIOR = Keyword("PRIOR")
  protected val SIBLINGS = Keyword("SIBLINGS")
  protected val START = Keyword("START")
  protected val SET = Keyword("SET")
  protected val MATCH = Keyword("MATCH")

  /* Context-based keywords */
  protected val CTX_LEVELS = "levels"
  protected val CTX_PATH = "path"
  protected val CTX_NAME = "name"
  protected val CTX_LEVEL = "level"

  /* Describe table keyword */
  protected val OLAP_DESCRIBE = Keyword("OLAP_DESCRIBE")

  /* Views keywords */
  protected val CREATE = Keyword("CREATE")
  protected val VIEW = Keyword("VIEW")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val DIMENSION = Keyword("DIMENSION")
  protected val CUBE = Keyword("CUBE")
  protected val EXISTS = Keyword("EXISTS")
  // IF is not added as keyword since it is also used as a function name
  protected val IF = ident.filter(_.toLowerCase == "if")

  /* Extract keywords */
  protected val EXTRACT = Keyword("EXTRACT")

  lexical.delimiters += "$"

  /* System table keywords */
  protected lazy val SYS = Keyword("SYS")
  protected lazy val OPTIONS = Keyword("OPTIONS")

  /* Infer schema keywords */
  protected lazy val INFER = ident.map(lexical.normalizeKeyword).filter(_ == "infer")
  protected lazy val SCHEMA = ident.map(lexical.normalizeKeyword).filter(_ == "schema")
  protected lazy val OF = ident.map(lexical.normalizeKeyword).filter(_ == "of")
  protected lazy val PARQUET = ident.map(lexical.normalizeKeyword).filter(_ == "parquet")
  protected lazy val ORC = ident.map(lexical.normalizeKeyword).filter(_ == "orc")

  protected lazy val optionName: Parser[String] = repsep(ident, ".").^^(_.mkString("."))

  protected lazy val pair: Parser[(String, String)] =
    optionName ~ stringLit ^^ { case k ~ v => (k, v) }

  /** Parses the content of OPTIONS and puts the result in a case insensitive map */
  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ (s => new CaseInsensitiveMap(s.toMap))

  /**
   * This is copied from [[projection]] but extended to allow annotations
   * on the attributes.
   */
  protected lazy val projection: Parser[Expression] =
    expression ~ (AS ~> ident) ~ metadata ^^ {
      case e ~ a ~ k => AnnotatedAttribute(Alias(e, a)())(k)
    } |
    expression ~ metadataFilter ^^ {
      case e ~ f => AnnotationFilter(e)(f)
    } |
    rep1sep(ident, ".") ~ metadata ^^ {
      case e ~ k =>
        AnnotatedAttribute(Alias(UnresolvedAttribute(e), e.last)())(k)
    } |
    expression ~ (AS.? ~> ident.?) ^^ {
      case e ~ a => a.fold(e)(Alias(e, _)())
    }

  /**
   * This is the starting rule from, where parsing always starts.
   *
   * Overriden to hook [[createView]] parser.
   */
  override protected lazy val start: Parser[LogicalPlan] =
    selectUsing | start1 | insert | cte | createViewUsing | createView | inferSchema

  /**
    * This is the starting rule for select statements.
    *
    * Overridden to hook [[selectUsing]] parser.
    */
  protected lazy val start1: Parser[LogicalPlan] =
  (selectUsing | select | ("(" ~> select <~ ")")) *
    (UNION ~ ALL        ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Union(q1, q2) } |
     INTERSECT          ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Intersect(q1, q2) } |
     EXCEPT             ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Except(q1, q2)} |
     UNION ~ DISTINCT.? ^^^ { (q1: LogicalPlan, q2: LogicalPlan) => Distinct(Union(q1, q2))})

  protected lazy val select: Parser[LogicalPlan] =
    SELECT ~> DISTINCT.? ~
      repsep(projection, ",") ~
      (FROM   ~> relations).? ~
      (WHERE  ~> expression).? ~
      (GROUP  ~  BY ~> rep1sep(expression, ",")).? ~
      (HAVING ~> expression).? ~
      sortType.? ~
      (LIMIT  ~> expression).? ^^ {
      case d ~ p ~ r ~ f ~ g ~ h ~ o ~ l =>
        val base = r.getOrElse(OneRowRelation)
        val withFilter = f.map(Filter(_, base)).getOrElse(base)
        val withProjection = g
          .map(Aggregate(_, p.map(UnresolvedAlias), withFilter))
          .getOrElse(Project(p.map(UnresolvedAlias), withFilter))
        val withDistinct = d.map(_ => Distinct(withProjection)).getOrElse(withProjection)
        val withHaving = h.map(Filter(_, withDistinct)).getOrElse(withDistinct)
        val withOrder = o.map(_(withHaving)).getOrElse(withHaving)
        val withLimit = l.map(Limit(_, withOrder)).getOrElse(withOrder)
        withLimit
    }

  protected lazy val insert: Parser[LogicalPlan] =
    INSERT ~> (OVERWRITE ^^^ true | INTO ^^^ false) ~ (TABLE ~> relation) ~ select ^^ {
      case o ~ r ~ s =>
        InsertIntoTable(r, Map.empty[String, Option[String]], s, o, ifNotExists = false)
    }

  protected lazy val cte: Parser[LogicalPlan] =
    WITH ~> rep1sep(ident ~ (AS ~ "(" ~> start1 <~ ")"), ",") ~ (start1 | insert) ^^ {
      case r ~ s => With(s, r.map({case n ~ i => (n, Subquery(n, i))}).toMap)
    }

  /**
   * Overriden to hook [[hierarchy]] parser.
   */
  protected lazy val relation: Parser[LogicalPlan] =
    hierarchy | joinedRelation | relationFactor

  /**
   * Every function / expression parsing is hooked here.
   *
   * @note Do not add rules to parse new functions here unless
   *       they have special syntax. Functions with standard
   *       syntax should be registered with [[SQLContext.functionRegistry]].
   *       See [[RegisterCustomFunctions]].
   */
  protected lazy val function: Parser[Expression] =
    extract | originalFunction | dataSourceFunctions

  /** Hierarchy parser. */
  protected lazy val hierarchy: Parser[LogicalPlan] =
    (HIERARCHY ~> "(" ~> hierarchySpec <~ ")") ~ (AS ~> ident) ^^ {
      case lp ~ ident => Subquery(ident, lp)
    }


  protected lazy val hierarchySpec: Parser[LogicalPlan] =
    (adjacencyListHierarchy ~ (SET ~> ident) ^^ {
      case (lp, child, exp, st, sort) ~ ident =>
        Hierarchy(
          AdjacencyListHierarchySpec(source = lp, childAlias = child, parenthoodExp = exp,
            startWhere = st, orderBy = sort),
          node = UnresolvedAttribute(ident))
    }
      | levelBasedHierarchy ~ (SET ~> ident) ^^ {
      case (lp, levels, matcher, st, sort) ~ ident =>
        Hierarchy(
          LevelBasedHierarchySpec(source = lp, levels = levels, matcher = matcher,
            startWhere = st, orderBy = sort),
          node = UnresolvedAttribute(ident))
    })

  protected lazy val adjacencyListHierarchy: Parser[(LogicalPlan, String, Expression,
    Option[Expression], Seq[SortOrder])] = {
    (USING ~> relationFactor) ~ (JOIN ~> PRIOR ~> ident) ~ (ON ~> expression) ~
      hierarchySpecOptions ^^ {
      case source ~ child ~ expr ~ ((searchBy, startWhere)) =>
        (source, child, expr, searchBy, startWhere)
    }
  }

  protected lazy val levelBasedHierarchy: Parser[(LogicalPlan, Seq[Expression],
    LevelMatcher, Option[Expression], Seq[SortOrder])] = {
    (USING ~> relationFactor) ~ (WITH ~> ident ~ identifiers) ~
      matchExpression ~ hierarchySpecOptions ^^ {
      case source ~ (levelsKeyword ~ levels) ~ matcher ~
        ((searchBy, startWhere)) if lexical.normalizeKeyword(
        levelsKeyword) == CTX_LEVELS =>
        (source, levels, matcher, searchBy, startWhere)
    }
  }

  /**
    * Parser of the 'MATCH' clause. Here a small trick is done to parse specific keywords
    * *without* adding them to the parser's preserved keyword list as they might be used the
    * user, therefor we try to parse them from the context. More importantly, we adhere to Spark's
    * rules of case-sensitivity by using the lexical and not the implied ''String'' parser.
    */
  protected lazy val matchExpression: Parser[LevelMatcher] =
    MATCH ~> ident ^^ {
      case name if lexical.normalizeKeyword(name) == CTX_PATH => MatchPath
      case name if lexical.normalizeKeyword(name) == CTX_NAME => MatchName
    }|MATCH ~> (ident ~ ident) ^^ {
      case name1 ~ name2 if lexical.normalizeKeyword(name1) == CTX_LEVEL &&
        lexical.normalizeKeyword(name2) == CTX_NAME =>
        MatchLevelName
    }

  protected lazy val hierarchySpecOptions: Parser[(Option[Expression], Seq[SortOrder])] =
    (ORDER ~> SIBLINGS ~> BY ~> ordering).? ~ (START ~> WHERE ~> expression).? ^^ {
      case orderBy ~ startWhere => (startWhere, orderBy.getOrElse(Seq()))
    }

  protected lazy val identifiers: Parser[Seq[Expression]] =
    "(" ~> rep1sep(ident, ",") <~ ")" ^^ { seq =>
      seq.map(UnresolvedAttribute(_))
    }

  protected lazy val viewKind: Parser[ViewKind] = (DIMENSION | CUBE).? <~ VIEW ^^ {
    case ViewKind(kind) => kind
  }

  /** Create temporary [dimension] view parser. */
  protected lazy val createView: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.?) ~ viewKind ~ (ident <~ AS) ~ start1 ^^ {
      case temp ~ kind ~ name ~ query =>
        CreateNonPersistentViewCommand(kind, TableIdentifier(name), query, temp.isDefined)
    }

  /**
    * Resolves a CREATE VIEW ... USING statement, in addition is adds the original VIEW SQL string
    * added by the user into the OPTIONS.
    */
  protected lazy val createViewUsing: Parser[CreatePersistentViewCommand] =
    withConsumedInput(
      (CREATE ~> viewKind) ~ (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~ (AS ~> start1) ~
      (USING ~> className) ~ (OPTIONS ~> options).?) ^^ {
      case (kind ~ allowExisting ~ name ~ plan ~ provider ~ opts, text) =>
        CreatePersistentViewCommand(
          kind = kind,
          identifier = name,
          plan = plan,
          viewSql = text.trim,
          provider = provider,
          options = opts.getOrElse(CaseInsensitiveMap.empty),
          allowExisting = allowExisting.isDefined)
    }

  protected lazy val inferSchema: Parser[UnresolvedInferSchemaCommand] =
    INFER ~> SCHEMA ~> OF ~> stringLit ~ (AS ~> fileType).? ^^ {
      case path ~ explicitFileType => UnresolvedInferSchemaCommand(path, explicitFileType)
    }

  protected lazy val fileType: Parser[FileType] =
    PARQUET ^^^ Parquet | ORC ^^^ Orc

  /** EXTRACT function. */
  protected lazy val extract: Parser[Expression] =
    EXTRACT ~ "(" ~> extractPart ~ (FROM ~> expression) <~ ")" ^^ { case f ~ d => f(d) }

  /** @see [[extract]] */
  protected lazy val extractPart: Parser[Expression => Expression] =
    "(?i)DAY".r ^^^ { e: Expression => DayOfMonth(e) } |
    "(?i)MONTH".r ^^^ { e: Expression => Month(e) } |
    "(?i)YEAR".r ^^^ { e: Expression => Year(e) } |
    "(?i)HOUR".r ^^^  { e: Expression => Hour(e) } |
    "(?i)MINUTE".r ^^^ { e: Expression => Minute(e) } |
    "(?i)SECOND".r ^^^ { e: Expression => Second(e) }

  protected lazy val relationFactor: Parser[LogicalPlan] =
    sysTable |
    ident ~ ("(" ~> repsep(start1, ",") <~ ")") ^^ {
      case name ~ arguments =>
        UnresolvedTableFunction(name, arguments)
    } |
    (
      tableIdentifier ~ (opt(AS) ~> opt(ident)) ^^ {
          case tableIdent ~ alias => UnresolvedRelation(tableIdent, alias)
      } |
      ("(" ~> start <~ ")") ~ (AS.? ~> ident) ^^ { case s ~ a => Subquery(a, s) }
    )

  /**
    * Parser for system table name. A system table name can be:
    * 1. qualified name starting with SYS (e.g. SYS.TABLES).
    * 2. table name starting with SYS_ (e.g. SYS_TABLES).
    */
  protected lazy val sysTableName: Parser[String] =
    // case-insensitive SYS_ followed by identifier.
    """(?i:SYS)_([a-zA-Z][a-zA-Z_0-9]*)""".r ^^ { col => col.drop("SYS_".length) } |
    SYS ~> "." ~> ident ^^ { name => name }

  protected lazy val sysTable: Parser[UnresolvedSystemTable] =
    sysTableName ~ ((USING ~> repsep(ident, ".")) ~ (OPTIONS ~> options).?).? ^^ {
      case name ~ Some(provider ~ opts) =>
        UnresolvedProviderBoundSystemTable(
          name,
          provider.mkString("."),
          opts.getOrElse(Map.empty[String, String]))
      case name ~ None =>
        UnresolvedSparkLocalSystemTable(name)
    }

  /**
   * Parser for data source specific functions. That is, functions
   * prefixed with $, so that they are always push down to the data
   * source as they are.
   */
  protected lazy val dataSourceFunctions: Parser[Expression] =
    "$" ~> ident ~ ("(" ~> repsep(expression, ",") <~ ")") ^^ {
      case udf ~ expr => DataSourceExpression(udf.toLowerCase, expr)
    }

  /*
   * TODO: Remove in Spark 1.4.1/1.5.0. This fixes NOT operator precendence, which we
   *       need for some SqlLogicTest queries.
   *       https://issues.apache.org/jira/browse/SPARK-6740
   */
  protected lazy val andExpression: Parser[Expression] =
    booleanFactor * (AND ^^^ { (e1: Expression, e2: Expression) => And(e1, e2) })

  protected lazy val booleanFactor: Parser[Expression] =
    NOT.? ~ comparisonExpression ^^ {
      case notOpt ~ expr => notOpt.map(s => Not(expr)).getOrElse(expr)
    }

  protected lazy val comparisonExpression: Parser[Expression] =
    termExpression ~ ("="  ~> termExpression) ^^ { case e1 ~ e2 => EqualTo(e1, e2) } |
    termExpression ~ ("<"  ~> termExpression) ^^ { case e1 ~ e2 => LessThan(e1, e2) } |
    termExpression ~ ("<=" ~> termExpression) ^^ { case e1 ~ e2 => LessThanOrEqual(e1, e2) } |
    termExpression ~ (">"  ~> termExpression) ^^ { case e1 ~ e2 => GreaterThan(e1, e2) } |
    termExpression ~ (">=" ~> termExpression) ^^ { case e1 ~ e2 => GreaterThanOrEqual(e1, e2) } |
    termExpression ~ ("!=" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) } |
    termExpression ~ ("<>" ~> termExpression) ^^ { case e1 ~ e2 => Not(EqualTo(e1, e2)) } |
    termExpression ~ ("<=>" ~> termExpression) ^^ { case e1 ~ e2 => EqualNullSafe(e1, e2) } |
    termExpression ~ NOT.? ~ (BETWEEN ~> termExpression) ~ (AND ~> termExpression) ^^ {
      case e ~ not ~ el ~ eu =>
        val betweenExpr: Expression = And(GreaterThanOrEqual(e, el), LessThanOrEqual(e, eu))
        not.fold(betweenExpr)(f => Not(betweenExpr))
    } |
    termExpression ~ (RLIKE  ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) } |
    termExpression ~ (REGEXP ~> termExpression) ^^ { case e1 ~ e2 => RLike(e1, e2) } |
    termExpression ~ (LIKE   ~> termExpression) ^^ { case e1 ~ e2 => Like(e1, e2) } |
    termExpression ~ (NOT ~ LIKE ~> termExpression) ^^ { case e1 ~ e2 => Not(Like(e1, e2)) } |
    termExpression ~ (IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
      case e1 ~ e2 => In(e1, e2)
    } |
    termExpression ~ (NOT ~ IN ~ "(" ~> rep1sep(termExpression, ",")) <~ ")" ^^ {
      case e1 ~ e2 => Not(In(e1, e2))
    } |
    termExpression <~ IS ~ NULL ^^ IsNull |
    termExpression <~ IS ~ NOT ~ NULL ^^ IsNotNull |
    termExpression

  /**
    * For special engine integration syntax
    */
  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ (_.mkString("."))

  /** A parser which matches a raw sql literal */
  def rawSqlLit: Parser[String] =
    elem("raw sql literal", _.isInstanceOf[lexical.RawSqlLiteral]) ^^ (_.chars)

  /**
    * Parses RAW Sql, i.e., sql we do not parse but pass directly to an appropriate datasource
    *
    * Example: ``some engine specific syntax`` USING com.sap.spark.engines [OPTIONS (key "value")]
    *
     */
  protected lazy val selectUsing: Parser[LogicalPlan] =
    rawSqlLit ~ (USING ~> className) ~ (OPTIONS ~> options).? ~ (AS ~> tableCols).? ^^ {
      case command ~ provider ~ optionsOpt ~ columnsOpt =>
        UnresolvedSelectUsing(
          command,
          provider,
          columnsOpt.map(StructType.apply),
          optionsOpt.getOrElse(Map.empty))
  }

  /**
    * Copied from Spark DDL Parser
    */
  protected lazy val tableCols: Parser[Seq[StructField]] = "(" ~> repsep(column, ",") <~ ")"

  protected val COMMENT = Keyword("COMMENT")

  protected lazy val column: Parser[StructField] =
    ident ~ dataType ~ (COMMENT ~> stringLit).?  ^^ { case columnName ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(COMMENT.str.toLowerCase, comment).build()
        case None => Metadata.empty
      }

      StructField(columnName, typ, nullable = true, meta)
    }

  // Based very loosely on the MySQL Grammar.
  // http://dev.mysql.com/doc/refman/5.0/en/join.html
  protected lazy val relations: Parser[LogicalPlan] =
    relation ~ rep1("," ~> relation) ^^ {
      case r1 ~ joins => joins.foldLeft(r1) {
        case (lhs, r) => Join(lhs, r, Inner, None)
      }
    } | relation

  protected lazy val joinedRelation: Parser[LogicalPlan] =
    relationFactor ~ rep1(joinType.? ~ (JOIN ~> relationFactor) ~ joinConditions.?) ^^ {
      case r1 ~ joins =>
        joins.foldLeft(r1) { case (lhs, jt ~ rhs ~ cond) =>
          Join(lhs, rhs, joinType = jt.getOrElse(Inner), cond)
        }
    }

  protected lazy val joinConditions: Parser[Expression] =
    ON ~> expression

  protected lazy val joinType: Parser[JoinType] =
    INNER ^^^ Inner |
    LEFT  ~ SEMI ^^^ LeftSemi |
    LEFT  ~ OUTER.? ^^^ LeftOuter |
    RIGHT ~ OUTER.? ^^^ RightOuter |
    FULL  ~ OUTER.? ^^^ FullOuter

  protected lazy val sortType: Parser[LogicalPlan => LogicalPlan] =
    ORDER ~ BY ~> ordering ^^ (o => (l: LogicalPlan) => Sort(o, global = true, l)) |
    SORT ~ BY ~> ordering ^^ (o => (l: LogicalPlan) => Sort(o, global = false, l))

  protected lazy val ordering: Parser[Seq[SortOrder]] =
    rep1sep(expression ~ direction.? , ",") ^^ { exps =>
      exps.map {
        case child ~ directionOpt => SortOrder(child, directionOpt.getOrElse(Ascending))
      }
    }

  protected lazy val direction: Parser[SortDirection] =
    ASC ^^^ Ascending | DESC ^^^ Descending

  protected lazy val expression: Parser[Expression] =
    orExpression

  protected lazy val orExpression: Parser[Expression] =
    andExpression * (OR ^^^ { (e1: Expression, e2: Expression) => Or(e1, e2) })

  protected lazy val notExpression: Parser[Expression] =
    NOT.? ~ comparisonExpression ^^ { case maybeNot ~ e => maybeNot.map(_ => Not(e)).getOrElse(e) }

  protected lazy val termExpression: Parser[Expression] =
    productExpression *
      ("+" ^^^ { (e1: Expression, e2: Expression) => Add(e1, e2) } |
       "-" ^^^ { (e1: Expression, e2: Expression) => Subtract(e1, e2) })

  protected lazy val productExpression: Parser[Expression] =
    baseExpression *
      ("*" ^^^ { (e1: Expression, e2: Expression) => Multiply(e1, e2) } |
       "/" ^^^ { (e1: Expression, e2: Expression) => Divide(e1, e2) } |
       "%" ^^^ { (e1: Expression, e2: Expression) => Remainder(e1, e2) } |
       "&" ^^^ { (e1: Expression, e2: Expression) => BitwiseAnd(e1, e2) } |
       "|" ^^^ { (e1: Expression, e2: Expression) => BitwiseOr(e1, e2) } |
       "^" ^^^ { (e1: Expression, e2: Expression) => BitwiseXor(e1, e2) })

  /* XXX: RENAMED function to originalFunction */
  protected lazy val originalFunction: Parser[Expression] =
    ident <~ ("(" ~ "*" ~ ")") ^^ { udfName =>
      if (lexical.normalizeKeyword(udfName) == "count") {
        AggregateExpression(Count(Literal(1)), mode = Complete, isDistinct = false)
      } else {
        throw new AnalysisException(s"invalid expression $udfName(*)")
      }
    } |
    ident ~ ("(" ~> repsep(expression, ",")) <~ ")" ^^ {
      case udfName ~ exprs => UnresolvedFunction(udfName, exprs, isDistinct = false)
    } |
    ident ~ ("(" ~ DISTINCT ~> repsep(expression, ",")) <~ ")" ^^ {
      case udfName ~ exprs =>
        lexical.normalizeKeyword(udfName) match {
          case "count" =>
            aggregate.Count(exprs).toAggregateExpression(isDistinct = true)
          case _ => UnresolvedFunction(udfName, exprs, isDistinct = true)
        }
    } |
    APPROXIMATE ~> ident ~ ("(" ~ DISTINCT ~> expression <~ ")") ^^ {
      case udfName ~ exp =>
        if (lexical.normalizeKeyword(udfName) == "count") {
          AggregateExpression(new HyperLogLogPlusPlus(exp), mode = Complete, isDistinct = false)
        } else {
          throw new AnalysisException(s"invalid function approximate $udfName")
        }
    } |
    APPROXIMATE ~> "(" ~> unsignedFloat ~ ")" ~ ident ~ "(" ~ DISTINCT ~ expression <~ ")" ^^ {
      case s ~ _ ~ udfName ~ _ ~ _ ~ exp =>
        if (lexical.normalizeKeyword(udfName) == "count") {
          AggregateExpression(
            HyperLogLogPlusPlus(exp, s.toDouble, 0, 0),
            mode = Complete,
            isDistinct = false)
        } else {
          throw new AnalysisException(s"invalid function approximate($s) $udfName")
        }
    } |
    CASE ~> whenThenElse ^^ CaseWhen |
    CASE ~> expression ~ whenThenElse ^^ {
      case keyPart ~ branches => CaseKeyWhen(keyPart, branches)
    }

  protected lazy val whenThenElse: Parser[List[Expression]] =
    rep1(WHEN ~> expression ~ (THEN ~> expression)) ~ (ELSE ~> expression).? <~ END ^^ {
      case altPart ~ elsePart =>
        altPart.flatMap { case whenExpr ~ thenExpr =>
          Seq(whenExpr, thenExpr)
        } ++ elsePart
    }

  protected lazy val cast: Parser[Expression] =
    CAST ~ "(" ~> expression ~ (AS ~> dataType) <~ ")" ^^ {
      case exp ~ t => Cast(exp, t)
    }


  protected lazy val baseExpression: Parser[Expression] =
    "*" ^^^ UnresolvedStar(None) |
    rep1(ident <~ ".") <~ "*" ^^ (target => UnresolvedStar(Option(target))) |
    primary

  protected lazy val signedPrimary: Parser[Expression] =
    sign ~ primary ^^ { case s ~ e => if (s == "-") UnaryMinus(e) else e }

  protected lazy val attributeName: Parser[String] = acceptMatch("attribute name", {
    case lexical.Identifier(str) => str
    case lexical.Keyword(str) if !lexical.delimiters.contains(str) => str
  })

  protected lazy val primary: PackratParser[Expression] =
    literal |
    expression ~ ("[" ~> expression <~ "]") ^^ {
      case base ~ ordinal => UnresolvedExtractValue(base, ordinal)
    } |
    (expression <~ ".") ~ ident ^^ {
      case base ~ fieldName => UnresolvedExtractValue(base, Literal(fieldName))
    } |
    cast |
    "(" ~> expression <~ ")" |
    function |
    dotExpressionHeader |
    signedPrimary |
    "~" ~> expression ^^ BitwiseNot |
    attributeName ^^ UnresolvedAttribute.quoted

  protected lazy val dotExpressionHeader: Parser[Expression] =
    (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest => UnresolvedAttribute(Seq(i1, i2) ++ rest)
    }

  protected lazy val tableIdentifier: Parser[TableIdentifier] =
    (ident <~ ".").? ~ ident ^^ {
      case maybeDbName ~ tableName => TableIdentifier(tableName, maybeDbName)
    }

  /**
    * Main entry point for the parser.
    *
    * Overriden to control error handling.
    *
    * @param input Query to be parsed.
    * @return A [[LogicalPlan]].
    */
  override def parse(input: String): LogicalPlan = {
    /*
     * This is a workaround to a race condition in AbstractSparkSQLParser:
     * https://issues.apache.org/jira/browse/SPARK-8628
     */
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
}
