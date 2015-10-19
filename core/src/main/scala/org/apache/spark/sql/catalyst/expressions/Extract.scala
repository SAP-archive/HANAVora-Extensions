package org.apache.spark.sql.catalyst.expressions

import java.sql.{Date, Timestamp}
import java.util.{Calendar, Locale, TimeZone}

import org.apache.spark.sql.catalyst.expressions.DateFlag._
import org.apache.spark.sql.types._

case class Extract(dFlag: Expression, d: Expression) extends Expression {

  override type EvaluatedType = Integer

  if (d.resolved && !supportedDataType(d.dataType)) {
    sys.error(s"Unsupported data type for extract: ${d.dataType} (requires DATE or TIMESTAMP")
  }
  if (dFlag.resolved && dFlag.dataType != StringType) {
    sys.error(s"Unsupported data type for date part: ${d.dataType} (requires STRING)")
  }

  private def supportedDataType(dt: DataType): Boolean = dt match {
    case TimestampType | DateType => true
    case _ => false
  }

  override def eval(input: Row): EvaluatedType = {
    val dateEval = d.eval(input)
    val flagEval = dFlag.eval(input)

    if (flagEval == null || dateEval == null) {
      null
    } else {
      val flag = DateFlag.withName(flagEval.asInstanceOf[String])
      val calendar = Calendar.getInstance(TimeZone.getTimeZone("Etc/UTC"), Locale.ENGLISH)
      dateEval match {
        case days: Integer =>
          calendar.setTimeInMillis(0)
          calendar.add(Calendar.DAY_OF_MONTH, days)
        case date: Date =>
          calendar.setTimeInMillis(date.getTime)
        case dateTime: Timestamp =>
          calendar.setTimeInMillis(dateTime.getTime)
        case other => sys.error(s"Unexpected type ${other.getClass}")
      }
      doExtract(calendar, flag)
    }
  }

  private def doExtract(calendar: Calendar, flag: DateFlag): Integer = flag match {
    case DAY => calendar.get(Calendar.DAY_OF_MONTH)
    case MONTH => calendar.get(Calendar.MONTH) + 1
    case YEAR => calendar.get(Calendar.YEAR)
    case HOUR => calendar.get(Calendar.HOUR_OF_DAY)
    case MINUTE => calendar.get(Calendar.MINUTE)
    case SECOND => calendar.get(Calendar.SECOND)
  }

  override def nullable: Boolean = dFlag.nullable || d.nullable

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = dFlag :: d :: Nil
}

