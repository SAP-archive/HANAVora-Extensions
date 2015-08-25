package org.apache.spark.sql.util


object ParseErrorFormatter {

  /**
   * Generates a simple but non-misleading error message
   * for a parse error. E.g.,
   *
   * ---------------------------------------------------
   * > Syntax error after line 1, column 37
   * >
   * > CREATE TEMPORARY TABLE table001 (a1 int_, a2 int)
   * >                                     ^
   * ---------------------------------------------------
   *
   * @param input The SQL/DDL/DML input string
   * @param nLine line(s) being successfully parsed
   * @param nCol column(s) being successfully parsed
   * @return An error message stating up to which position
   *         the parser successfully parsed the input.
   *
   */
  def nonmisleadingParseErrorMessage(input: String, nLine: Int, nCol: Int): String = {
    val inputWithPosition =
        input.split("\n").patch(nLine, Seq(" " * (nCol - 1) + "^"), 0).mkString("\n")
    s"Syntax error at or near line $nLine, column $nCol\n\n$inputWithPosition"
  }
}
