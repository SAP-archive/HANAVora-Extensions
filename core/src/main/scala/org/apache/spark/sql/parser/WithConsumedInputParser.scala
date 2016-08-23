package org.apache.spark.sql.parser

import scala.util.parsing.combinator.Parsers

trait WithConsumedInputParser {
  this: Parsers =>

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
