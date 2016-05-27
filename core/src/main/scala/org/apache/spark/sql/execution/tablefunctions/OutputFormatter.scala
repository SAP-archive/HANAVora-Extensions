package org.apache.spark.sql.execution.tablefunctions

class OutputFormatter(val inputValues: Any*) {

  def prepend(values: Any*): OutputFormatter = new OutputFormatter(values ++ inputValues)

  def append(values: Any*): OutputFormatter = new OutputFormatter(inputValues ++ values)

  def format(): Seq[Seq[Any]] = {
    inputValues.foldLeft(Seq.empty[Seq[Any]]) {
      case (acc, seq: Seq[_]) =>
        seq.flatMap { value =>
          val target = nonEmptyOrElse(acc)(Seq(Seq()))
          target.map(_ :+ value)
        }
      case (acc, map: Map[_, _]) =>
        map.flatMap {
          case (key, value) =>
            nonEmptyOrElse(acc)(Seq(Seq())).map(_ ++ (key :: value :: Nil))
        }.toSeq
      case (acc, value) =>
        nonEmptyOrElse(acc)(Seq(Seq())).map(_ :+ value)
    }
  }

  private def nonEmptyOrElse[A](s: Seq[A])(default: => Seq[A]): Seq[A] = s match {
    case Seq() => default
    case nonEmpty => nonEmpty
  }
}

