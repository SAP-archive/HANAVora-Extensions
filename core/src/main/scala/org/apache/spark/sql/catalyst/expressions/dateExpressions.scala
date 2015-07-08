package org.apache.spark.sql.catalyst.expressions

import java.sql
import java.sql.Timestamp
import java.util.Calendar
import java.util.Date
import org.apache.spark.sql.types.{DataType, LongType, IntegerType}
import org.apache.spark.sql.types.DateType
import java.util.TimeZone
import java.util.Locale

/** Return the current date */
case class CurDate() extends Expression {

  override type EvaluatedType = Date

  override def eval(input: Row): EvaluatedType = {
    Calendar.getInstance.getTime
  }

  override def nullable: Boolean = false
  override def dataType: DataType = DateType
  override def children: Seq[Expression] = Nil
}

/** Extract date part */
case class DatePart(ed: Expression, part: Integer) extends Expression {

  override type EvaluatedType = Integer

  override def eval(input: Row): EvaluatedType = {
    val date = ed.eval(input)
    if (date == null) {
      0
    } else {
      val calendar = Calendar.getInstance
      calendar.clear()
      
      date match {
        case dateTime: Timestamp =>
          calendar.setTimeInMillis(dateTime.getTime)
        case days: Integer =>
          calendar.setTimeInMillis(0)
          calendar.add(Calendar.DAY_OF_MONTH, days)
        case l: Long =>
          calendar.setTimeInMillis(l)
        case date: Date =>
          calendar.setTimeInMillis(date.getTime)
        case other =>
          sys.error(s"Type ${other.getClass} does not support date operations")
      }
      val p = calendar.get(part)
      if (part == Calendar.MONTH){
        p + 1
      }
      else {
        p
      }
    }
  }

  override def nullable: Boolean = ed.nullable
  override def dataType: DataType = IntegerType
  override def children: Seq[Expression] = ed :: Nil
}

/** Return ed plus en days as new date */
case class AddDays(ed: Expression, en: Expression) extends Expression {

  override type EvaluatedType = sql.Date

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val d = ed.eval(input)
    if (d == null) {
      null
    } else {
      val n : Long = en.eval(input) match {
        case null => 0
        case d : Double => d.toLong * (24*3600*1000)   
        case l : Long   => l * (24*3600*1000)  
        case i : Integer=> i * (24*3600*1000)
        case f : Float  => f.toLong * (24*3600*1000)
        case other =>
          sys.error(s"Type $other does not support date operations")
      }
      new sql.Date((d match {
        case ts: Timestamp => ts.getTime
        case i : Integer => i.toLong * (24*3600*1000)
        case l : Long => l
        case dt: Date => dt.getTime
        case other =>
          sys.error(s"Type ${other.getClass} does not support date operations")
      }) + n)
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = ed.nullable
  override def dataType: DataType = DateType
  override def children: Seq[Expression] = ed :: en :: Nil
}

/** Return ed plus en months as new date */
case class AddMonths(ed: Expression, en: Expression) extends Expression {

  override type EvaluatedType = sql.Date

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val date = ed.eval(input)
    if (date == null) {
      null
    } else {
      val calendar = Calendar.getInstance(TimeZone.getTimeZone("Etc/UTC"), Locale.ENGLISH)
      calendar.clear()
      
      date match {
        case dt: Timestamp =>
          calendar.setTimeInMillis(dt.getTime)
        case days: Integer =>
          calendar.setTimeInMillis(0)
          calendar.add(Calendar.DAY_OF_MONTH, days)
        case l: Long =>
          calendar.setTimeInMillis(l)
        case date: Date =>
          calendar.setTimeInMillis(date.getTime)
        case other =>
          sys.error(s"Type ${other.getClass} does not support date operations")
      }
      val n = en.eval(input)
      if (n!= null) {
        val count : Integer = n match {
          case d : Double => d.toInt  
          case l : Long   => l.toInt  
          case i : Integer=> i
          case f : Float  => f.toInt
          case other =>
            sys.error(s"Type ${other.getClass} does not support date operations")
        }
        calendar.add (Calendar.MONTH, count)
      }
      new sql.Date(calendar.getTimeInMillis)
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = ed.nullable
  override def dataType: DataType = DateType
  override def children: Seq[Expression] = ed :: en :: Nil
}

/** Return ed plus en yeaars as new date */
case class AddYears(ed: Expression, en: Expression) extends Expression {

  override type EvaluatedType = sql.Date

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val date = ed.eval(input)
    if (date == null) {
      null
    } else {
      val calendar = Calendar.getInstance(TimeZone.getTimeZone("Etc/UTC"), Locale.ENGLISH)
      calendar.clear()
      
      date match {
        case dt: Timestamp =>
          calendar.setTimeInMillis(dt.getTime)
        case days: Integer =>
          calendar.setTimeInMillis(0)
          calendar.add(Calendar.DAY_OF_MONTH, days)
        case l: Long =>
          calendar.setTimeInMillis(l)
        case date: Date =>
          calendar.setTimeInMillis(date.getTime)
        case other =>
          sys.error(s"Type ${other.getClass} does not support date operations")
      }
      val n = en.eval(input)
      if (n!= null) {
        val count : Integer = n match {
          case d : Double => d.toInt   
          case l : Long   => l.toInt   
          case i : Integer=> i
          case f : Float  => f.toInt
          case other =>
            sys.error(s"Type ${other.getClass} does not support date operations")
        }
        calendar.add (Calendar.YEAR, count)
      }
      new sql.Date(calendar.getTimeInMillis)
    }
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = ed.nullable
  override def dataType: DataType = DateType
  override def children: Seq[Expression] = ed :: en :: Nil
}

/** Return the number of days between ed1 and ed1 */
case class DaysBetween(ed1: Expression, ed2: Expression) extends Expression {

  override type EvaluatedType = Long

  // scalastyle:off cyclomatic.complexity
  override def eval(input: Row): EvaluatedType = {
    val date1 = ed1.eval(input)
    val date2 = ed2.eval(input)
    (date1,date2) match {
      case (null,_) | (_,null) | (null,null) => 0
      case (_,_) =>
        val calendar1 = Calendar.getInstance
        calendar1.clear()
        
        date1 match {
          case dt1: Timestamp =>
            calendar1.setTimeInMillis(dt1.getTime)
          case i1: Integer =>
            calendar1.setTimeInMillis(0)
            calendar1.add(Calendar.DAY_OF_MONTH, i1)
          case l1: Long =>
            calendar1.setTimeInMillis(l1)
          case date: Date =>
            calendar1.setTimeInMillis(date.getTime)
          case other =>
            sys.error(s"Type ${other.getClass} does not support date operations")
        }
        
        val calendar2 = Calendar.getInstance
        calendar2.clear()
        
        date2 match {
          case dt2: Timestamp =>
            calendar2.setTimeInMillis(dt2.getTime)
          case i2: Integer =>
            calendar2.setTimeInMillis(0)
            calendar2.add(Calendar.DAY_OF_MONTH, i2)
          case l2: Long =>
            calendar2.setTimeInMillis(l2)
          case date: Date =>
            calendar2.setTimeInMillis(date.getTime)
          case other =>
            sys.error(s"Type ${other.getClass} does not support date operations")
        }
        (calendar2.getTimeInMillis - calendar1.getTimeInMillis)/(24*3600*1000)
    }    
  }
  // scalastyle:on cyclomatic.complexity

  override def nullable: Boolean = ed1.nullable || ed2.nullable 
  override def dataType: DataType = LongType
  override def children: Seq[Expression] = ed1 :: ed2 :: Nil
}

