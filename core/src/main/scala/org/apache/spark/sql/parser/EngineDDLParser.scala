package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.commands.RawDDLCommand
import org.apache.spark.sql.sources.{RawDDLObjectType, RawDDLStatementType}
import org.apache.spark.sql.types._

/**
  * A parser extension for engine DDL.
  * Contains rules for partition functions, partition schemes, graph, document (collection), and
  * timeseries DDL.
  */
private[sql] trait EngineDDLParser extends BackportedSapSqlParser {

  protected val DROP = Keyword("DROP")
  protected val APPEND = Keyword("APPEND")
  protected val PARTITION = Keyword("PARTITION")
  protected val PARTITIONED = Keyword("PARTITIONED")
  protected val PARTITIONS = Keyword("PARTITIONS")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val RANGE = Keyword("RANGE")
  protected val BOUNDARIES = Keyword("BOUNDARIES")
  protected val HASH = Keyword("HASH")
  protected val USE = Keyword("USE")
  protected val MIN = Keyword("MIN")
  protected val MAX = Keyword("MAX")
  protected val AUTO = Keyword("AUTO")
  protected val BLOCK = Keyword("BLOCK")
  protected val BLOCKSIZE = Keyword("BLOCKSIZE")
  protected val SCHEME = Keyword("SCHEME")
  protected val COLOCATION = Keyword("COLOCATION")
  protected val GRAPH = Keyword("GRAPH")
  protected val COLLECTION = Keyword("COLLECTION")
  protected val SERIES = Keyword("SERIES")
  protected val PERIOD = Keyword("PERIOD")
  protected val FOR = Keyword("FOR")
  protected val MINVALUE = Keyword("MINVALUE")
  protected val MAXVALUE = Keyword("MAXVALUE")
  protected val POPULATE = Keyword("POPULATE")
  protected val EQUIDISTANT = Keyword("EQUIDISTANT")
  protected val INCREMENT = Keyword("INCREMENT")
  protected val DATE = Keyword("DATE")
  protected val TIME = Keyword("TIME")
  protected val YEAR = Keyword("YEAR")
  protected val MONTH = Keyword("MONTH")
  protected val DAY = Keyword("DAY")
  protected val HOUR = Keyword("HOUR")
  protected val MINUTE = Keyword("MINUTE")
  protected val SECOND = Keyword("SECOND")
  protected val MILLISECOND = Keyword("MILLISECOND")
  protected val TIMESTAMP = Keyword("TIMESTAMP")
  protected val MISSING = Keyword("MISSING")
  protected val ALLOWED = Keyword("ALLOWED")
  protected val VALUES = Keyword("VALUES")
  protected val ROUNDING = Keyword("ROUNDING")
  protected val DEFAULT = Keyword("DEFAULT")
  protected val COMPRESSION = Keyword("COMPRESSION")
  protected val APCA = Keyword("APCA")
  protected val SDT = Keyword("SDT")
  protected val SPLINE = Keyword("SPLINE")
  protected val PERCENT = Keyword("PERCENT")
  protected val LOAD = Keyword("LOAD")
  protected val ANY = Keyword("ANY")
  protected val NONE = Keyword("NONE")
  protected val ERROR = Keyword("ERROR")

  /**
    * needed to parse column names with ellipsis
    */
  lexical.delimiters += ".."

  def combineString(strs: String*): String =
    strs.find(s => !s.isEmpty).mkString(" ")

  // engine extensions
  protected lazy val enginePartitionFunction: Parser[LogicalPlan] =
    partitionFunctionDefinition ~ (USING ~> className) ^^ {
      case partitionFunction ~ clazz =>
        val (identifier, query) = partitionFunction
        RawDDLCommand(
          identifier,
          RawDDLObjectType.PartitionFunction,
          RawDDLStatementType.Create,
          None,
          query,
          clazz,
          Map.empty[String, String])
    }

  protected lazy val enginePartitionScheme: Parser[LogicalPlan] =
    partitionSchemeDefinition ~ (USING ~> className) ^^ {
      case partitionScheme ~ clazz =>
        val (identifier, query) = partitionScheme
        RawDDLCommand(
          identifier,
          RawDDLObjectType.PartitionScheme,
          RawDDLStatementType.Create,
          None,
          query,
          clazz,
          Map.empty[String, String])
    }

  protected lazy val engineGraphDefinition: Parser[LogicalPlan] =
    graphDefinition ~ (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case graph ~ clazz ~ opts =>
        graph match {
          case (identifier, query) =>
            RawDDLCommand(
              identifier,
              RawDDLObjectType.Graph,
              RawDDLStatementType.Create,
              None,
              query,
              clazz,
              opts.getOrElse(Map.empty[String, String]))
        }
    }

  protected lazy val engineCollectionDefinition: Parser[LogicalPlan] =
    collectionDefinition ~ (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case collection ~ clazz ~ opts =>
        val (identifier, query) = collection
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Collection,
          RawDDLStatementType.Create,
          None,
          query,
          clazz,
          opts.getOrElse(Map.empty[String, String]))
    }

  protected lazy val engineSeriesDefinition: Parser[LogicalPlan] =
    seriesDefinition ~ (USING ~> className) ~ (OPTIONS ~> options).? ^^ {
      case series ~ clazz ~ opts =>
      val (identifier, query, sparkSchema) = series
          RawDDLCommand(
            identifier,
            RawDDLObjectType.Series,
            RawDDLStatementType.Create,
            Some(sparkSchema),
            query,
            clazz,
            opts.getOrElse(Map.empty[String, String]))
      }

  protected lazy val engineDropGraph: Parser[LogicalPlan] =
    DROP ~ GRAPH ~ identOptQuotes ~ (USING ~> className) ^^ {
      case drop ~ graph ~ identifier ~ clazz =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Graph,
          RawDDLStatementType.Drop,
          None,
          s"$drop $graph $identifier",
          clazz,
          Map.empty[String, String])
    }

  protected lazy val engineDropCollection: Parser[LogicalPlan] =
    DROP ~ COLLECTION ~ identOptQuotes ~ (USING ~> className) ^^ {
      case drop ~ graph ~ identifier ~ clazz =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Collection,
          RawDDLStatementType.Drop,
          None,
          s"$drop $graph $identifier",
          clazz,
          Map.empty[String, String])
    }

  protected lazy val engineDropSeries: Parser[LogicalPlan] =
    DROP ~ (SERIES ~> TABLE) ~ identOptQuotes ~ (USING ~> className) ^^ {
      case drop ~ table ~ identifier ~ clazz =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Series,
          RawDDLStatementType.Drop,
          None,
          s"$drop $table $identifier",
          clazz,
          Map.empty[String, String])
    }

  protected lazy val engineAppendGraph: Parser[LogicalPlan] =
    (APPEND ~> GRAPH ~> identOptQuotes) ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case identifier ~ clazz ~ opts =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Graph,
          RawDDLStatementType.Append,
          None,
          "",
          clazz,
          opts)
    }

  protected lazy val engineAppendCollection: Parser[LogicalPlan] =
    (APPEND ~> COLLECTION ~> identOptQuotes) ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case identifier ~ clazz ~ opts =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Collection,
          RawDDLStatementType.Append,
          None,
          "",
          clazz,
          opts)
    }

  protected lazy val engineAppendSeries: Parser[LogicalPlan] =
    (APPEND ~> SERIES ~> TABLE ~> identOptQuotes) ~
      (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case identifier ~ classname ~ opts =>
        RawDDLCommand(
          identifier,
          RawDDLObjectType.Series,
          RawDDLStatementType.Append,
          None,
          "",
          classname,
          opts)
    }

  // note(mathis): following grammar rules adapted to hanalite-parser/src/v2/parser
  protected lazy val partitionFunctionDefinition: Parser[(String, String)] =
    (CREATE ~ PARTITION ~ FUNCTION ~ identOptQuotes ~ "(" ~ partitionFunctionParameterList ~ ")" ~
      AS ~ partitionFunctionFunctionDefinitionList ~ ("," ~> partitionFunctionAuto).? ^^ {
      case create ~ partition ~ function ~ ident ~
        brace1 ~ parameters ~ brace2 ~ as ~ definitions ~ auto =>
        val prefix = s"$create $partition $function $ident " +
                     s"$brace1 $parameters $brace2 $as $definitions"
        ident ->
          Seq(
            Some(prefix),
            auto
          ).flatten.mkString(", ")
      }
    |CREATE ~ PARTITION ~ FUNCTION ~ identOptQuotes ~ "(" ~ partitionFunctionParameterList ~ ")" ~
      AS ~ partitionFunctionAuto ^^ {
      case create ~ partition ~ function ~ ident ~ brace1 ~ parametersr ~ brace2 ~ as ~ auto =>
        (ident,
         s"$create $partition $function $ident $brace1 $parametersr $brace2 $as $auto")
      }
    )

  protected lazy val partitionFunctionParameterList: Parser[String] =
    partitionFunctionColumnList | partitionFunctionIdentifierDefinitionList

  protected lazy val partitionFunctionColumnList: Parser[String] =
    rep1sep(partitionFunctionColumnDefinition, ",") ^^ {
      case columnDefinition => columnDefinition.mkString(", ")
    }

  protected lazy val partitionFunctionColumnDefinition: Parser[String] =
    identOptQuotes ~ dataTypeExt ^^ {
      case columnName ~ typ =>
        val (typeString, dataType) = typ
        s"$columnName $typeString"
    }

  protected lazy val partitionFunctionIdentifierDefinitionList: Parser[String] =
    rep1sep(identifierChain, ",") ^^ {
      case identifierChains =>
        identifierChains.mkString(", ")
    }

  protected lazy val partitionFunctionFunctionDefinitionList: Parser[String] =
    rep1sep(partitionFunctionFunctionDefinition, ",") ^^ {
      case functionDefinitions =>
        functionDefinitions.mkString(", ")
    }

  protected lazy val partitionFunctionFunctionDefinition: Parser[String] =
    partitionFunctionRange | partitionFunctionHash | partitionFunctionBlock

  protected lazy val partitionFunctionRange: Parser[String] =
    RANGE ~ ("(" ~> identifierChain <~ ")").? ~ partitionFunctionRangeBoundaryDefinition.? ~
      partitionFunctionMinPartitions.? ~ partitionFunctionMaxPartitions.? ^^ {
      case range ~ identifier ~ rangeBoundary ~ min ~ max =>
        Seq(
          Some("range"),
          identifier.map(id => s"( $id )"),
          rangeBoundary,
          min,
          max).flatten.mkString(" ")
    }

  // todo: allow general expression lists if necessary
  protected lazy val partitionFunctionRangeBoundaryDefinition =
    BOUNDARIES ~ "(" ~ boundariesList ~ ")" ^^ {
      case boundaries ~ brace1 ~ bounds ~ brace2 =>
        s"$boundaries $brace1 $bounds $brace2"
    }

  protected lazy val partitionFunctionMinPartitions: Parser[String] =
    MIN ~ PARTITIONS ~ integral ^^ {
      case min ~ partitions ~ integerLiteral =>
        s"$min $partitions ${toNarrowestIntegerType(integerLiteral)}"
    }

  protected lazy val partitionFunctionMaxPartitions: Parser[String] =
    MAX ~ PARTITIONS ~ integral ^^ {
      case max ~ partitions ~ integerLiteral =>
        s"$max $partitions ${toNarrowestIntegerType(integerLiteral)}"
    }

  protected lazy val partitionFunctionPartitions: Parser[String] =
    PARTITIONS ~ integral ^^ {
      case partitions ~ integerLiteral =>
        s"$partitions ${toNarrowestIntegerType(integerLiteral)}"
    }

  protected lazy val partitionFunctionHash: Parser[String] =
    HASH ~ ("(" ~> identifierNameList <~ ")").? ~
      partitionFunctionMinPartitions.? ~ partitionFunctionMaxPartitions.? ^^ {
      case hash ~ names ~ min ~ max =>
        Seq(
          Some("hash"),
          names.map(n => s"( $n )"),
          min,
          max
        ).flatten.mkString(" ")
    }

  protected lazy val identifierNameList: Parser[String] =
    identifierChainList

  protected lazy val partitionFunctionBlock: Parser[String] =
    BLOCK ~ "(" ~ identifierNameList ~ ")" ~ partitionFunctionPartitions ~
      partitionFunctionBlocksize ^^ {
      case block ~ brace1 ~ names ~ brace2 ~ parts ~ blockSize =>
        s"$block $brace1 $names $brace2 $parts $blockSize"
    }

  protected lazy val partitionFunctionBlocksize: Parser[String] =
    BLOCKSIZE ~ integral ^^ {
      case blockSize ~ integralLiteral =>
        val literal = Literal(toNarrowestIntegerType(integralLiteral))
        s"$blockSize $literal"
    }

  protected lazy val partitionFunctionAuto: Parser[String] =
    AUTO

  protected lazy val partitionSchemeDefinition: Parser[(String, String)] =
    CREATE ~ PARTITION ~ SCHEME ~ identOptQuotes ~
      USING ~ identOptQuotes ~ (WITH ~> COLOCATION).? ^^ {
      case create ~ partition ~ scheme ~ ident1 ~ using ~ ident2 ~ colocation =>
        ident1 ->
          Seq(
            Some(s"create partition scheme $ident1 using $ident2"),
            colocation.map(definition => s"with $definition")
          ).flatten.mkString(" ")
    }

  protected lazy val graphDefinition: Parser[(String, String)] =
    CREATE ~ GRAPH ~ identifierChain ~ partitionClause.? ^^ {
      case create ~ graph ~ identifier ~ partition =>
        identifier ->
          Seq(
            Some(s"$create $graph $identifier"),
            partition
          ).flatten.mkString(" ")
    }

  protected lazy val collectionDefinition: Parser[(String, String)] =
    CREATE ~ COLLECTION ~ identifierChain ~ partitionClause.? ^^ {
      case create ~ collection ~ identifier ~ partition =>
        identifier ->
          Seq(
            Some(s"$create $collection $identifier"),
            partition
          ).flatten.mkString(" ")
    }

  protected lazy val seriesDefinition: Parser[(String, String, StructType)] =
    CREATE ~ TABLE ~ (IF ~> NOT <~ EXISTS).? ~ identifierChain ~
      tableColsNoAnnotation ~ seriesClause ~ partitionClause.? ^^ {
      case create ~ table ~ not ~ identifier ~ columns ~ series ~ partition =>
        val (columnString, columnStructType) = columns
        val ddlString =
          Seq(
            Some("create table"),
            not.map(_ => "if not exists"),
            Some(s"$identifier $columnString $series"),
            partition
          ).flatten.mkString(" ")

        (identifier, ddlString, columnStructType)
    }

  protected lazy val partitionClause: Parser[String] =
    (PARTITION | PARTITIONED) ~> BY ~> identOptQuotes ~ "(" ~ identifierNameList ~ ")" ^^ {
      case identifier ~ brace1 ~ names ~ brace2 =>
        s"partition by $identifier $brace1 $names $brace2"
    }

  protected lazy val tableColsNoAnnotation: Parser[(String, StructType)] =
    "(" ~ repsep(columnDataTypeExt, ",") ~ ")" ^^ {
      case brace1 ~ cols ~ brace2 =>
        val stringResult =
          s"$brace1 ${cols.map(c => c._1).mkString(", ")} $brace2"
        val structResult = StructType(cols.map(c => c._2))
        (stringResult, structResult)
    }

  protected lazy val columnDataTypeExt: Parser[(String, StructField)] =
    identOptQuotes ~ dataTypeExt ~ (COMMENT ~> stringLit).? ^^ {
      case identifier ~ datatype ~ comment =>
        val (datatypeString, datatypeType) = datatype
        val stringResult =
          Seq(
            Some(s"$identifier $datatypeString"),
            comment.map(c => s"${COMMENT.str.toLowerCase()} '$c'")
          ).flatten.mkString(" ")

        val meta = comment match {
          case Some(cm) =>
            new MetadataBuilder().putString(COMMENT.str.toLowerCase, cm).build()
          case None => Metadata.empty
        }
        val fieldResult = StructField(identifier, datatypeType, nullable = true, meta)
        (stringResult, fieldResult)
    }

  // todo: move type mapping to analysis phase
  // todo: proper handling of time type
  protected lazy val dataTypeExt: Parser[(String, DataType)] =
    (primitiveType ^^ {
      case s: StringType => ("varchar(*)", StringType)
      case default => (default.simpleString, default)
      }
    | TIME ^^ {
      case _ =>
        ("time", TimestampType)
      }
    )

  protected lazy val seriesClause: Parser[String] =
    SERIES ~ "(" ~ PERIOD ~ FOR ~ SERIES ~ identOptQuotes ~ rangeExpression.? ~
      equidistantDefinition.? ~ compressionClause.? ~ ")" ^^ {
      case series1 ~ brace1 ~ period ~ foor ~
        series2 ~ identifier ~ range ~ equidistant ~ compression ~ brace2 =>
        Seq(
          Some(s"$series1 $brace1 $period $foor $series2 $identifier"),
          range,
          equidistant,
          compression.flatten,
          Some(s"$brace2")
        ).flatten.mkString(" ")
    }

  protected lazy val rangeExpression: Parser[String] =
    rangeDefinition ~ populationClause.? ^^ {
      case range ~ population =>
        Seq(
          Some(range),
          population
        ).flatten.mkString(" ")
    }

  protected lazy val rangeDefinition: Parser[String] =
    (START ~ seriesTime ~ END ~ seriesTime ^^ {
      case start ~ time1 ~ end ~ time2 =>
        s"$start $time1 $end $time2"
      }
    | MINVALUE ~ seriesTime ~ MAXVALUE ~ seriesTime ^^ {
      case min ~ time1 ~ max ~ time2 =>
        s"$min $time1 $max $time2"
      }
    )

  protected lazy val seriesTime: Parser[String] =
    dateLiteral | timeLiteral | timestampLiteral

  protected lazy val dateLiteral: Parser[String] =
    DATE ~ stringLit ^^ {
      case date ~ stringLiteral =>
        s"$date '$stringLiteral'"
    }

  protected lazy val timeLiteral: Parser[String] =
    TIME ~ stringLit ^^ {
      case time ~ stringLiteral =>
        s"$time '$stringLiteral'"
    }

  protected lazy val timestampLiteral: Parser[String] =
    TIMESTAMP ~ stringLit ^^ {
      case timestamp ~ stringLiteral =>
        s"$timestamp '$stringLiteral'"
    }

  protected lazy val populationClause: Parser[String] =
    populateValue

  protected lazy val populateValue: Parser[String] =
    NOT.? ~ POPULATE ^^ {
      case nnot ~ populate =>
        Seq(
          nnot,
          Some(populate)
        ).flatten.mkString(" ")
    }

  protected lazy val equidistantDefinition: Parser[String] =
    EQUIDISTANT ~ INCREMENT ~ BY ~ equidistantIntervalConst ~
      missingElements.? ~ loadExpression.? ^^ {
      case equidistant ~ increment ~ by ~ interval ~ missing ~ load =>
        Seq(
          Some(s"$equidistant $increment $by $interval"),
          missing,
          load
        ).flatten.mkString(" ")
    }

  protected lazy val equidistantIntervalConst: Parser[String] =
    granulizeIntervalConst

  protected lazy val granulizeIntervalConst: Parser[String] =
    numericLiteral ~ (YEAR | MONTH | DAY | HOUR | MINUTE | SECOND | MILLISECOND) ^^ {
      case numLiteral ~ label =>
        numLiteral.toString + s" $label"
    }

  protected lazy val missingElements: Parser[String] =
    MISSING ~ VALUES ~ allowedValue ^^ {
      case missing ~ values ~ allowed =>
        s"$missing $values $allowed"
    }

  protected lazy val allowedValue: Parser[String] =
    NOT.? ~ ALLOWED ^^ {
      case nnot ~ allowed =>
        Seq(
          nnot,
          Some(allowed)
        ).flatten.mkString(" ")
    }

  protected lazy val loadExpression: Parser[String] =
    ROUNDING ~ ON ~ LOAD ~ allowedValue ^^ {
      case rounding ~ on ~ load ~ allowed =>
        s"$rounding $on $load $allowed"
    }

  protected lazy val compressionClause: Parser[Option[String]] =
    defaultCompression.? ~ compressionDefinitionList.? ^^ {
      case None ~ None =>
        None
      case default ~ compression =>
        Some(
          Seq(
            default,
            compression
          ).flatten.mkString(" ")
        )
    }

  protected lazy val defaultCompression: Parser[String] =
    DEFAULT ~ COMPRESSION ~ USE ~ "(" ~ compressionType ~ ")" ^^ {
      case default ~ compression ~ use ~ brace1 ~ typ ~ brace2 =>
        s"$default $compression $use $brace1 $typ $brace2"
    }

  protected lazy val compressionType: Parser[String] =
    (NONE
    | compressionIdentifier ~ errorBound.? ^^ {
      case compression ~ error =>
        Seq(
          Some(compression),
          error
        ).flatten.mkString(" ")
      }
    )

  protected lazy val compressionIdentifier: Parser[String] =
    AUTO | APCA | SDT | SPLINE

  protected lazy val errorBound: Parser[String] =
    ERROR ~ unsignedFloat ~ PERCENT ^^ {
      case error ~ floatLiteral ~ percent =>
        s"$error ${floatLiteral.toString} $percent"
    }

  protected lazy val compressionDefinitionList: Parser[String] =
    rep1(compressionDefinition) ^^ {
      case compression =>
        compression.mkString(" ")
    }

  protected lazy val compressionDefinition: Parser[String] =
    COMPRESSION ~ ON ~ columnDef ~ compRangeDefList ^^ {
      case compression ~ on ~ columnDefinition ~ range =>
        s"$compression $on $columnDefinition $range"
    }

  protected lazy val columnDef: Parser[String] =
    (
      identOptQuotes
      | "(" ~ columnNameListWithEllipsis ~ ")" ^^ {
        case brace1 ~ columnName ~ brace2 =>
          s"$brace1 $columnName $brace2"
      }
    )

  protected lazy val columnNameListWithEllipsis: Parser[String] =
    rep1sep(columnNameWithEllipsis, ",") ^^ {
      case columnNames =>
        columnNames.mkString(", ")
    }

  protected lazy val columnNameWithEllipsis: Parser[String] =
    repsep(identOptQuotes, "..") ^^ {
      case identifier =>
        identifier.mkString("..")
    }

  protected lazy val compRangeDefList: Parser[String] =
    rep1sep(compRangeDef, ",") ^^ {
      case range =>
        range.mkString(", ")
    }

  protected lazy val compRangeDef: Parser[String] =
    rangeDef.? ~ USE ~ "(" ~ compressionType ~ ")" ^^ {
      case range ~ use ~ brace1 ~ compression ~ brace2 =>
        Seq(
          range,
          Some(s"$use $brace1 $compression $brace2")
        ).flatten.mkString(" ")
    }

  protected lazy val rangeDef: Parser[String] =
    BETWEEN ~ seriesTime ~ AND ~ seriesTime ^^ {
      case between ~ time1 ~ and ~ time2 =>
        s"$between $time1 $and $time2"
    }

  protected lazy val identifierChainList: Parser[String] =
    rep1sep(identifierChain, ",") ^^ {
      case identifier =>
        identifier.mkString(", ")
    }

  protected lazy val identifierChain: Parser[String] =
    (ANY
    | rep1sep(identOptQuotes, ".") ~ "." ~ "(" ~ nestedProjectionList ~ ")" ^^ {
      case identifier ~ dot ~ brace1 ~ nested ~ brace2 =>
        s"${identifier.mkString(".")}$dot$brace1$nested$brace2"
      }
    | rep1sep(identOptQuotes, ".") ~ "[" ~ integral ~ "]" ^^ {
      case identifier ~ brace1 ~ number ~ brace2 =>
        val literal = Literal(toNarrowestIntegerType(number))
        s"${identifier.mkString(".")}$brace1$literal$brace2"
      }
    | rep1sep(identOptQuotes, ".") ~ "." ~ ANY ^^ {
      case identifier ~ dot ~ any =>
        s"${identifier.mkString(".")}$dot$any"
      }
    | rep1sep(identOptQuotes, ".") ^^ {
      case identifiers =>
        identifiers.mkString(".")
      }
    | identOptQuotes ^^ {
      case identifier =>
        identifier.toString
      }
    )

  // todo(mathis): remove the ability to add quotes around identifiers
  //               once we have a deep integration of the new engine types
  //               Although the rule below is not compliant with SparkSQL, we keep it, as
  //               we want to allow users to make use of case sensitive identifiers across
  //               the new engine types for now.
  protected lazy val identOptQuotes: Parser[String] =
    (
      ident ^^ {
        case identifier =>
          identifier
      }
    |
      stringLit ^^ {
        stringLiteral =>
          s""""$stringLiteral""""
      }
    )

  protected lazy val nestedProjectionList: Parser[String] =
    rep1sep(identifierChain, ",") ^^ {
      case identifier =>
        identifier.mkString(", ")
    }

  protected lazy val boundariesList: Parser[String] =
    rep1sep(boundary, ",") ^^ {
      case boundaries =>
        boundaries.mkString(", ")
    }

  protected lazy val boundary: Parser[String] =
    (integral ^^ {
      case integerLiteral =>
        Literal(toNarrowestIntegerType(integerLiteral)).toString()
      }
    | dateLiteral
    | timeLiteral
    | timestampLiteral
    )
}
