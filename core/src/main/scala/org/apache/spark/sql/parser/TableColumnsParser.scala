package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

trait TableColumnsParser
  extends AbstractSparkSQLParser
  with DataTypeParser
  with AnnotationParser {

  protected def commentIndicator: Keyword

  protected lazy val columnName = acceptMatch("column name", {
    case lexical.Identifier(chars) => chars
    case lexical.Keyword(chars) if !sqlReservedWords.contains(chars.toUpperCase) => chars
  })

  /**
    * Overridden to allow the user to add annotations on the table columns.
    */
  protected lazy val tableColumns: Parser[Seq[StructField]] =
    "(" ~> repsep(annotatedCol, ",") <~ ")"

  protected lazy val annotatedCol: Parser[StructField] =
    columnName ~ metadata ~ dataType ^^ {
      case name ~ md ~ typ =>
        StructField(name, typ, nullable = true, metadata = toTableMetadata(md))
    } |
    columnName ~ dataType ~ (commentIndicator ~> stringLit).?  ^^ { case name ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(commentIndicator.str.toLowerCase, comment).build()
        case None => Metadata.empty
      }

      StructField(name, typ, nullable = true, meta)
    }
}
