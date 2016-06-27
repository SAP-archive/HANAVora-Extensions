package org.apache.spark.sql.execution.tablefunctions

/**
  * Transforms input to a table-like result.
  *
  * @param inputValues The input.
  *
  * TODO (YH, AC) rethink the complexity and how easy it is to extend this class. The input and
  * output of this class is vague.
  */
class OutputFormatter(val inputValues: Any*) {

  def prepend(values: Any*): OutputFormatter = new OutputFormatter(values ++ inputValues)

  def append(values: Any*): OutputFormatter = new OutputFormatter(inputValues ++ values)

  /**
    * Iterates of `inputValues` and returns a sequence of sequences. The `InputValues` can
    * contain primitive elements, sequences, and map.
    *
    * @return Sequence of Sequence of a value.
    */
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

object OutputFormatter {

  /**
    * Returns a map with `null -> null` in case the given map is empty, else the given map.
    *
    * @param map The map to check and return.
    * @tparam K The key type.
    * @tparam V The value type.
    * @return `Map(null -> null)` if the given map was empty, else the given map.
    */
  def toNonEmptyMap[K, V](map: Map[K, V]): Map[K, V] =
    if (map.isEmpty) {
      Map((null.asInstanceOf[K], null.asInstanceOf[V]))
    } else {
      map
    }
}

