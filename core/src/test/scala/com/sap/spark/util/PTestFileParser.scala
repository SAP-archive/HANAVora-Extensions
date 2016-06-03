package com.sap.spark.util

import scala.util.Try
import scala.util.parsing.combinator.RegexParsers

/**
  * Parser for files containing statements of the form:
  * "$query:<query>$parsed:<parsed>$expect:<expect>"
  *
  * ... where <query>, <parsed>, <expect> are strings.
  */
class PTestFileParser extends RegexParsers {

  def query: Parser[String] = """\$query:""".r ^^ { _.toString }
  def parsed: Parser[String] = """\$parsed:""".r ^^ { _.toString }
  def expect: Parser[String] = """\$expect:""".r ^^ { _.toString }
  def content: Parser[String] = """[^\$]*""".r ^^ { _.toString }

  def ptest: Parser[(String, String, String)] =
    (query ~> content) ~ (parsed ~> content).? ~ (expect ~> content) ^^ {
      case q ~ p ~ e =>
        (q.toString.trim, p.getOrElse("").trim, e.toString.trim)
    }

  def root: Parser[List[(String, String, String)]] =
    rep(ptest)

  def apply(input: String): Try[List[(String, String, String)]] =
    parseAll(root, input) match {
      case Success(result, _) => scala.util.Success(result)
      case Error(msg, _) => scala.util.Failure(new Exception("Could not parse PTest file " + msg))
      case Failure(msg, _) => scala.util.Failure(new Exception("Could not parse PTest file " + msg))
  }
}
