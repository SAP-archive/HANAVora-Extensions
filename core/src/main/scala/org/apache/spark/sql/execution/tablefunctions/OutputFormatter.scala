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
    * For a given [[Map]], all key-value pairs will be flattened and then each resulting sequence
    * will be cross-combined with the current sequences.
    *
    * Example:
    * Acc: `Seq(Seq(1), Seq(2))`
    * Value: `Map(1 -> 2, 2 -> 3)`
    * Result: `Seq(Seq(1, 1, 2), Seq(1, 2, 3), Seq(2, 1, 2), Seq(2, 2, 3))`
    *
    * For a given [[Seq]], all values are first flattened. Then, the resulting sequences are also
    * cross-combined with the current sequences.
    *
    * Example:
    * Acc: `Seq(Seq(1), Seq(2))`
    * Value: `Seq(Seq(1, 2), Seq(3, 4))`
    * Result: `Seq(Seq(1, 1, 2), Seq(2, 1, 2), Seq(1, 3, 4), Seq(2, 3, 4))`
    *
    * For all other values (primitives), they will be cross-combined with the current sequences.
    *
    * Example:
    * Acc: `Seq(Seq(1), Seq(2))`
    * Value: `3`
    * Result: `Seq(Seq(1, 3), Seq(2, 3))`
    *
    * @return Sequence of Sequence of a value.
    */
  def format(): Seq[Seq[Any]] = {
    inputValues.foldLeft(Seq.empty[Seq[Any]]) {
      case (acc, seq: Seq[_]) =>
        seq.flatMap { value =>
          val target = nonEmptyOrElse(acc)(Seq(Seq()))
          target.map(_ ++ flatten(value))
        }
      case (acc, map: Map[_, _]) =>
        map.flatMap {
          case (key, value) =>
            nonEmptyOrElse(acc)(Seq(Seq())).map(_ ++ (flatten(key) ++ flatten(value)))
        }.toSeq
      case (acc, value) =>
        nonEmptyOrElse(acc)(Seq(Seq())).map(_ ++ flatten(value))
    }
  }

  /**
    * Deeply flattens the given [[Seq]].
    *
    * Flattens the given [[Seq]] and any inner [[Seq]] to a single [[Seq]].
    * Example:
    * `flatten(Seq(Seq(1, Seq(2)), Seq(Seq(3))))` => `Seq(1, 2, 3)`
    *
    * @param v The [[Seq]] to flatten.
    * @return The flattened [[Seq]].
    */
  private def flatten(v: Any): Seq[Any] = v match {
    case s: Seq[_] => s.flatMap(flatten)
    case _ => Seq(v)
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

