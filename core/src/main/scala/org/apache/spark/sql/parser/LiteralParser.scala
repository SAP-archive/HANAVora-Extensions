package org.apache.spark.sql.parser

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.AbstractSparkSQLParser
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.util.DataTypeParser
import org.apache.spark.sql.types.{BooleanType, NullType, StringType}
import org.apache.spark.unsafe.types.CalendarInterval

trait LiteralParser extends AbstractSparkSQLParser with DataTypeParser {
  protected lazy val TRUE = Keyword("TRUE")
  protected lazy val FALSE = Keyword("FALSE")
  protected lazy val NULL = Keyword("NULL")
  protected lazy val INTERVAL = Keyword("INTERVAL")

  protected lazy val literal =
    (numericLiteral
      | booleanLiteral
      | stringLit ^^ (Literal.create(_, StringType))
      | intervalLiteral
      | NULL ^^^ Literal.create(null, NullType)
      )

  protected lazy val booleanLiteral =
    (TRUE ^^^ Literal.create(true, BooleanType)
      | FALSE ^^^ Literal.create(false, BooleanType)
      )

  protected lazy val numericLiteral =
    (integral  ^^ (i => Literal(toNarrowestIntegerType(i)))
      | sign.? ~ unsignedFloat ^^ {
      case s ~ f => Literal(toDecimalOrDouble(s.getOrElse("") + f))
    }
      )

  protected lazy val unsignedFloat =
    "." ~> numericLit ^^ { u => "0." + u } |
    elem("decimal", _.isInstanceOf[lexical.DecimalLit]) ^^(_.asInstanceOf[lexical.DecimalLit].chars)

  protected lazy val sign = "+" | "-"

  protected lazy val integral =
    sign.? ~ numericLit ^^ { case s ~ n => s.getOrElse("") + n }

  private def intervalUnit(unitName: String) =
    acceptIf {
      case lexical.Identifier(str) =>
        val normalized = lexical.normalizeKeyword(str)
        normalized == unitName || normalized == unitName + "s"
      case _ => false
    } {_ => "wrong interval unit"}

  protected lazy val month =
    integral <~ intervalUnit("month") ^^ (_.toInt)

  protected lazy val year =
    integral <~ intervalUnit("year") ^^ (_.toInt * 12)

  protected lazy val microsecond =
    integral <~ intervalUnit("microsecond") ^^ (_.toLong)

  protected lazy val millisecond =
    integral <~ intervalUnit("millisecond") ^^ (_.toLong * CalendarInterval.MICROS_PER_MILLI)

  protected lazy val second =
    integral <~ intervalUnit("second") ^^ (_.toLong * CalendarInterval.MICROS_PER_SECOND)

  protected lazy val minute =
    integral <~ intervalUnit("minute") ^^ (_.toLong * CalendarInterval.MICROS_PER_MINUTE)

  protected lazy val hour =
    integral <~ intervalUnit("hour") ^^ (_.toLong * CalendarInterval.MICROS_PER_HOUR)

  protected lazy val day =
    integral <~ intervalUnit("day") ^^ (_.toLong * CalendarInterval.MICROS_PER_DAY)

  protected lazy val week =
    integral <~ intervalUnit("week") ^^ (_.toLong * CalendarInterval.MICROS_PER_WEEK)

  private def intervalKeyword(keyword: String) = acceptIf {
    case lexical.Identifier(str) =>
      lexical.normalizeKeyword(str) == keyword
    case _ => false
  } {_ => "wrong interval keyword"}

  protected lazy val intervalLiteral =
    (INTERVAL ~> stringLit <~ intervalKeyword("year") ~ intervalKeyword("to") ~
      intervalKeyword("month") ^^ (s => Literal(CalendarInterval.fromYearMonthString(s)))
    | INTERVAL ~> stringLit <~ intervalKeyword("day") ~ intervalKeyword("to") ~
      intervalKeyword("second") ^^ (s => Literal(CalendarInterval.fromDayTimeString(s)))
    | INTERVAL ~> stringLit <~ intervalKeyword("year") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("year", s))
    } | INTERVAL ~> stringLit <~ intervalKeyword("month") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("month", s))
    } | INTERVAL ~> stringLit <~ intervalKeyword("day") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("day", s))
    } | INTERVAL ~> stringLit <~ intervalKeyword("hour") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("hour", s))
    } | INTERVAL ~> stringLit <~ intervalKeyword("minute") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("minute", s))
    } | INTERVAL ~> stringLit <~ intervalKeyword("second") ^^ { s =>
      Literal(CalendarInterval.fromSingleUnitString("second", s))
    } | INTERVAL ~> year.? ~ month.? ~ week.? ~ day.? ~ hour.? ~ minute.? ~ second.? ~
      millisecond.? ~ microsecond.? ^^ {
      case theYear ~ theMonth ~ theWeek ~ theDay ~ theHour ~ theMinute ~
              theSecond ~ theMillisecond ~ theMicrosecond =>
      if (Seq(
        theYear, theMonth, theWeek, theDay, theHour, theMinute, theSecond,
        theMillisecond, theMicrosecond).forall(_.isEmpty)) {
        throw new AnalysisException(
          "at least one time unit should be given for interval literal")
      }
      val months = Seq(theYear, theMonth).map(_.getOrElse(0)).sum
      val microseconds =
        Seq(theWeek, theDay, theHour, theMinute, theSecond, theMillisecond, theMicrosecond)
          .map(_.getOrElse(0L))
          .sum
      Literal(new CalendarInterval(months, microseconds))
    }
      )

  protected def toNarrowestIntegerType(value: String): Any = {
    val bigIntValue = BigDecimal(value)

    bigIntValue match {
      case v if bigIntValue.isValidInt => v.toIntExact
      case v if bigIntValue.isValidLong => v.toLongExact
      case v => v.underlying()
    }
  }

  protected def toDecimalOrDouble(value: String): Any = {
    val decimal = BigDecimal(value)
    // follow the behavior in MS SQL Server
    // https://msdn.microsoft.com/en-us/library/ms179899.aspx
    if (value.contains('E') || value.contains('e')) {
      decimal.doubleValue()
    } else {
      decimal.underlying()
    }
  }
}
