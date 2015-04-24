package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp
import java.util.Calendar

import org.apache.spark.sql.catalyst.expressions.DateFlag._
import org.apache.spark.sql.types.{DataType, IntegerType}

case class Extract(dFlag: Expression, d: Expression) extends Expression {

  override type EvaluatedType = Integer

  override def eval(input: Row): EvaluatedType = {

    val dateEval = d.eval(input)
    val flagEval = dFlag.eval(input)

    if (flagEval == null || dateEval == null) {
      null
    } else {
      val flag = DateFlag.withName(flagEval.asInstanceOf[String])

      val calendar = Calendar.getInstance
      calendar.clear

      dateEval match {
        case days: Integer =>
          calendar.setTimeInMillis(0)
          calendar.add(Calendar.DAY_OF_MONTH, days)
          doExtract(calendar, flag)
        case dateTime: Timestamp =>
          calendar.setTimeInMillis(dateTime.getTime)
          doExtract(calendar, flag)
        case other => null
      }
    }

  }

  private def doExtract(calendar: Calendar, flag: DateFlag): Integer = {
    flag match {
      case DAY => calendar.get(Calendar.DAY_OF_MONTH)
      case MONTH => calendar.get(Calendar.MONTH) + 1
      case YEAR => calendar.get(Calendar.YEAR)
      case HOUR => calendar.get(Calendar.HOUR_OF_DAY)
      case MINUTE => calendar.get(Calendar.MINUTE)
      case SECOND => calendar.get(Calendar.SECOND)
      case other => null
    }
  }

  override def nullable: Boolean = dFlag.nullable || d.nullable

  override def dataType: DataType = IntegerType

  override def children: Seq[Expression] = dFlag :: d :: Nil
}

