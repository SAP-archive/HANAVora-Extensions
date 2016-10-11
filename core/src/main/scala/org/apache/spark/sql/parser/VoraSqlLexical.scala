package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.SqlLexical

import scala.util.parsing.input.CharArrayReader._

/**
  * This is a modification to the SQL Lexical class, basically adding ``..`` as string literals.
  *
  * This is necessary for the RAW SQL execution using ``RAW STRING`` WITH DS Syntax
  */
class VoraSqlLexical extends SqlLexical {

    /** The class of RawSql literal tokens */
    case class RawSqlLiteral(chars: String) extends Token {
      override def toString: String = "\"" + chars + "\""
    }

    // Note: COPY from the SQLLexical (because we are unable to override
    override lazy val token: Parser[Token] =
      (rep1(digit) ~ scientificNotation ^^ { case i ~ s => DecimalLit(i.mkString + s) }
        | '.' ~> (rep1(digit) ~ scientificNotation) ^^
        { case i ~ s => DecimalLit("0." + i.mkString + s) }
        | rep1(digit) ~ ('.' ~> digit.*) ~ scientificNotation ^^
        { case i1 ~ i2 ~ s => DecimalLit(i1.mkString + "." + i2.mkString + s) }
        | digit.* ~ identChar ~ (identChar | digit).* ^^
        { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
        | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
        case i ~ None => NumericLit(i.mkString)
        case i ~ Some(d) => DecimalLit(i.mkString + "." + d.mkString)
      }
        | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
        { case chars => StringLit(chars mkString "") }
        | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
        { case chars => StringLit(chars mkString "") }
        | '{' ~> '{' ~> chrExcept('}', '\n', EofCh).* <~ '}' <~ '}' ^^
        { case chars => StringLit(chars mkString "") }
        | '`' ~> '`' ~> chrExcept('`', EofCh).* <~ '`' <~ '`' ^^
        { case chars => RawSqlLiteral(chars mkString "") }
        | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
        { case chars => Identifier(chars mkString "") }
        | EofCh ^^^ EOF
        | '\'' ~> failure("unclosed string literal")
        | '"' ~> failure("unclosed string literal")
        | delim
        | failure("illegal character")
        )

  // Note: COPY from the SQLLexical (because we are unable to override
    private lazy val scientificNotation: Parser[String] =
      (elem('e') | elem('E')) ~> (elem('+') | elem('-')).? ~ rep1(digit) ^^ {
        case s ~ rest => "e" + s.mkString + rest.mkString
      }

}
