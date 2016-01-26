package org.apache.spark.sql

import org.apache.spark.sql.catalyst.{AbstractSparkSQLParser, SqlLexical}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{AnnotationReference, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * a parser for OLAP annotations of attributes.
 */
private[sql] trait AnnotationParsingRules extends AbstractSparkSQLParser {

  // we want to use SqlLexical which offers more options compared to standard one.
  override val lexical = new SqlLexical

  lexical.delimiters += "?"

  /**
   * Parses meta data and returns a [[Map]] of keys and values.
   */
  protected lazy val metadata: Parser[Map[String, Expression]] =
    "@" ~> "(" ~> repsep(annotation, ",") <~ ")" ^^ { case kvs =>
      val duplicates =
        kvs.groupBy(_._1).map { case (k, v) => (k, v.map(_._2))}.filter(_._2.size > 1).keys
      if(duplicates.nonEmpty) {
        throw new AnalysisException(s"duplicate keys found: ${duplicates.mkString(",")}")
      }
      kvs.toMap
    }

  protected lazy val metadataFilter: Parser[Set[String]] =
    "@" ~> "(" ~> repsep(annotationFilter, ",") <~ ")" ^^ {case ks => ks.toSet}

  /**
   * Parses annotation key and value.
   */
  protected lazy val annotation: Parser[(String, Expression)] =
    annotationKey ~ ("=" ~> annotationValue) ^^ {
      case k ~ v => (k, v)
    }

  protected lazy val annotationFilter: Parser[String] =
    annotationKey <~ ("=" ~ "?") ^^ {
      case k => k
    }

  /**
   * Parses the annotation key.
   */
  protected lazy val annotationKey: Parser[String] =
    (qualifiedIdent
    |"*")

  protected lazy val qualifiedIdent: Parser[String] =
    rep1sep(ident, ".") ^^ {
      case k => k.mkString(".")}

  /**
   * Parses annotation value(s).
   */
  protected lazy val annotationValue: Parser[Expression] =
    singleAnnotationValue | stringArrayAnnotationValue

  protected lazy val stringArrayAnnotationValue: Parser[Expression] =
    "(" ~> rep1sep(stringLit, ",") <~ ")" ^^ {
      // (YH) Array[String] values are handled as simple Strings since there is
      // special requirements regarding handling of Array[String].
      case vs => Literal.create("[".concat(vs.mkString(",")).concat("]"), StringType)
  }

  protected lazy val floatLiteral: Parser[String] =
    ("." ~> numericLit ^^ { u => "0." + u }
      | elem("decimal", _.isInstanceOf[lexical.FloatLit]) ^^ (_.chars)
    )

  /**
   * Parses single annotation value.
   */
  protected lazy val singleAnnotationValue: Parser[Expression] =
    ("null" ^^ {case i => Literal.create(null, NullType)}
    |stringLit ^^ {case i => Literal.create(i, StringType)}
    |numericLit ^^ {case i => Literal.create(i.toLong, LongType)}
    |floatLiteral ^^ {case i => Literal.create(i.toDouble, DoubleType)}
    |annotationReference
    )

  protected lazy val annotationReference: Parser[Expression] =
    "@" ~> (ident <~ ".") ~ ident ~ rep("." ~> ident) ^^ {
      case i1 ~ i2 ~ rest =>
        val seq = Seq(i1, i2) ++ rest
        AnnotationReference(seq.last, UnresolvedAttribute(seq.dropRight(1)))
    }

  /**
   * Transforms a metadata map to [[Metadata]] object which is used in table attributes.
 *
   * @param metadata the metadata map.
   * @return the [[Metadata]] object.
   */
  protected def toTableMetadata(metadata: Map[String, Expression]): Metadata = {
    val res = new MetadataBuilder()
    metadata.foreach {
      case (k, v:Literal) =>
        v.dataType match {
          case StringType =>
            if (k.equals("?")) {
              sys.error("column metadata key can not be ?")
            }
            if (k.equals("*")) {
              sys.error("column metadata key can not be *")
            }
            res.putString(k, v.value.asInstanceOf[UTF8String].toString)
          case LongType => res.putLong(k, v.value.asInstanceOf[Long])
          case DoubleType => res.putDouble(k, v.value.asInstanceOf[Double])
          case NullType =>
            res.putString(k, null)
          case a:ArrayType => res.putString(k, v.value.toString)
        }
      case (k, v:AnnotationReference) =>
        sys.error("column metadata can not have a reference to another column metadata")
    }
    res.build()
  }
}
