package org.apache.spark.sql.catalyst.expressions

import java.util.{Calendar, TimeZone}
import org.apache.spark.sql.catalyst.compat.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.util.DateTimeUtils

import org.apache.spark.unsafe.types.compat._
import org.apache.spark.sql.catalyst.expressions.compat._
/* XXX: Preserve these imports. Required for Spark 1.5 compatibility. */
import org.apache.spark.sql.types.{DateType, DataType, TimestampType, StringType}
import org.apache.spark.sql.types.IntegerType

// scalastyle:off

//
// Backported from Spark 1.5.
//

/**
  * Returns the current date at the start of query evaluation.
  * All calls of current_date within the same query return the same value.
  *
  * There is no code generation since this expression should get constant folded by the optimizer.
  */
case class CurrentDate() extends BackportedLeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = DateType

  override def eval(input: InternalRow): Any = {
    DateTimeUtils.millisToDays(System.currentTimeMillis())
  }
}

/**
  * Returns the current timestamp at the start of query evaluation.
  * All calls of current_timestamp within the same query return the same value.
  *
  * There is no code generation since this expression should get constant folded by the optimizer.
  */
case class CurrentTimestamp() extends BackportedLeafExpression with CodegenFallback {
  override def foldable: Boolean = true
  override def nullable: Boolean = false

  override def dataType: DataType = TimestampType

  override def eval(input: InternalRow): Any = {
    System.currentTimeMillis() * 1000L
  }
}

/**
  * Adds a number of days to startdate.
  */
case class DateAdd(startDate: Expression, days: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] + d.asInstanceOf[Int]
  }

  /* XXX: REMOVED genCode */
}

/**
  * Subtracts a number of days to startdate.
  */
case class DateSub(startDate: Expression, days: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = startDate
  override def right: Expression = days

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, d: Any): Any = {
    start.asInstanceOf[Int] - d.asInstanceOf[Int]
  }

  /* XXX: REMOVED genCode */
}

case class Hour(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  /* XXX: timestamp in Spark 1.5 is Long, but java.sql.Timestamp in Spark 1.4. */
  override protected def nullSafeEval(input: Any): Any = {
    DateTimeUtils.getHours(DateTimeUtils.timestampToMicros(input))
  }

  /* XXX: REMOVED genCode */
}

case class Minute(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  /* XXX: timestamp in Spark 1.5 is Long, but java.sql.Timestamp in Spark 1.4. */
  override protected def nullSafeEval(input: Any): Any = {
    DateTimeUtils.getMinutes(DateTimeUtils.timestampToMicros(input))
  }

  /* XXX: REMOVED genCode */
}

case class Second(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(TimestampType)

  override def dataType: DataType = IntegerType

  /* XXX: timestamp in Spark 1.5 is Long, but java.sql.Timestamp in Spark 1.4. */
  override protected def nullSafeEval(input: Any): Any = {
    DateTimeUtils.getSeconds(DateTimeUtils.timestampToMicros(input))
  }

  /* XXX: REMOVED genCode */
}

case class DayOfYear(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayInYear(date.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}


case class Year(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getYear(date.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}

case class Quarter(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getQuarter(date.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}

case class Month(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getMonth(date.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}

case class DayOfMonth(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  override protected def nullSafeEval(date: Any): Any = {
    DateTimeUtils.getDayOfMonth(date.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}

case class WeekOfYear(child: Expression) extends BackportedUnaryExpression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType)

  override def dataType: DataType = IntegerType

  @transient private lazy val c = {
    val c = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
    c.setFirstDayOfWeek(Calendar.MONDAY)
    c.setMinimalDaysInFirstWeek(4)
    c
  }

  override protected def nullSafeEval(date: Any): Any = {
    c.setTimeInMillis(date.asInstanceOf[Int] * 1000L * 3600L * 24L)
    c.get(Calendar.WEEK_OF_YEAR)
  }

  /* XXX: REMOVED genCode */
}

/* XXX: REMOVED DateFormatClass, UnixTimestamp */

/* XXX: REMOVED FromUnixTime, LastDay, NextDay */

/* XXX: REMOVED TimeAdd (time interval not supported in Spark 1.4) */

/* XXX: REMOVED FromUTCTimestamp */

/* XXX: REMOVED TimeSub (time interval not supported in Spark 1.4) */

/**
  * Returns the date that is num_months after start_date.
  */
case class AddMonths(startDate: Expression, numMonths: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = startDate
  override def right: Expression = numMonths

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, IntegerType)

  override def dataType: DataType = DateType

  override def nullSafeEval(start: Any, months: Any): Any = {
    DateTimeUtils.dateAddMonths(start.asInstanceOf[Int], months.asInstanceOf[Int])
  }

  /* XXX: REMOVED genCode */
}

/* XXX: REMOVED MonthsBetween, ToUTCTimestamp, ToDate */

/**
  * Returns date truncated to the unit specified by the format.
  */
case class TruncDate(date: Expression, format: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {
  override def left: Expression = date
  override def right: Expression = format

  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, StringType)
  override def dataType: DataType = DateType
  override def prettyName: String = "trunc"

  private lazy val truncLevel: Int =
    DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])

  override def eval(input: InternalRow): Any = {
    val level = if (format.foldable) {
      truncLevel
    } else {
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
    }
    if (level == -1) {
      // unknown format
      null
    } else {
      val d = date.eval(input)
      if (d == null) {
        null
      } else {
        DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
      }
    }
  }

  /* XXX: REMOVED genCode */
}

/**
  * Returns the number of days from startDate to endDate.
  */
case class DateDiff(endDate: Expression, startDate: Expression)
  extends BackportedBinaryExpression with ImplicitCastInputTypes {

  override def left: Expression = endDate
  override def right: Expression = startDate
  override def inputTypes: Seq[AbstractDataType] = Seq(DateType, DateType)
  override def dataType: DataType = IntegerType

  override def nullSafeEval(end: Any, start: Any): Any = {
    end.asInstanceOf[Int] - start.asInstanceOf[Int]
  }

  /* XXX: REMOVED genCode */
}
