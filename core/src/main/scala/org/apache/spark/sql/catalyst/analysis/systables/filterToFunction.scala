package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.sources._

/**
  * Converts given filters to functions and optional remains.
  */
object filterToFunction {
  /**
    * Validates a row-like sequence of values and returns `true` if it is valid, `false` otherwise.
    */
  type Validation = Seq[Any] => Boolean

  /**
    * Converts the given [[Filter]] to a [[Validation]] and an [[Option]] of the remaining filter.
    *
    * If the given attributes do not cover all of the attributes contained in the [[Filter]],
    * there will be a remaining [[Filter]] object.
    *
    * @param filter The [[Filter]] to convert.
    * @param attributes The attributes to check for.
    * @return The remaining [[Filter]] that is not targeted by the given attributes and
    *         the [[Validation]].
    */
  def apply(filter: Filter, attributes: Seq[String]): (Option[Filter], Validation) = {
    val children = childrenOf(filter)
    val validation = toValidation(filter, attributes)

    if (areAttributesCoveredBy(filter, attributes)) {
      None -> validation
    } else if (children.isEmpty) {
      Some(filter) -> validation
    } else {
      optimizeMultipleChildFilter(filter, attributes, children, validation)
    }
  }

  /**
    * Optimizes [[Filter]]s that have multiple children.
    *
    * @param filter The [[Filter]] with multiple children.
    * @param attributes The attributes to generate a [[Validation]] for.
    * @param children The children of the [[Filter]].
    * @param validation The [[Validation]] of the given [[Filter]].
    * @return The remaining [[Filter]] as well as the generated [[Validation]].
    */
  private def optimizeMultipleChildFilter(filter: Filter,
                                          attributes: Seq[String],
                                          children: Set[Filter],
                                          validation: Validation): (Option[Filter], Validation) = {
    val childResults: Seq[Filter] =
      children.map(apply(_, attributes)).flatMap(_._1)(collection.breakOut)
    filter match {
      case and: And => childResults match {
        /**
          * In case one of the children of the [[And]] filter has been satisfied by the
          * given sequence of attributes, we can collapse the [[And]] to its remaining, unsatisfied
          * child.
          */
        case Seq(single) => Some(single) -> validation
        /**
          * In all other cases, we cannot break down the [[And]] and return it as remains alongside
          * with its [[Validation]].
          */
        case Seq(first, second) => Some(and) -> validation
      }
      /**
        * For other multi-child [[Filter]]s ([[Not]], [[Or]]) we cannot optimize and prematurely
        * extract a [[Validation]], thus we need to return an always-true [[Validation]].
        */
      case default => Some(default) -> ((s: Seq[Any]) => true)
    }
  }

  /**
    * Compares to [[Any]] values.
    *
    * In case of one long value, it tries to compare both values as longs.
    * In case of two string values, it compares these strings.
    * If no other case matched, both values are converted to [[Number]] and their
    * `doubleValue` is compared.
    *
    * @param v1 The first value
    * @param v2 The second value
    * @return An integer representing the comparison result of the two values.
    * @throws ClassCastException if one or both values could not be correctly casted.
    */
  private def compare(v1: Any, v2: Any): Int = (v1, v2) match {
    case (_: Long, _) | (_, _: Long) =>
      v1.asInstanceOf[Number].longValue().compareTo(v2.asInstanceOf[Number].longValue())
    case (s1: String, s2: String) => s1.compareTo(s2)
    case _ => v1.asInstanceOf[Number].doubleValue().compareTo(v2.asInstanceOf[Number].doubleValue())
  }

  /**
    * A null sensitive equal method. Can only compare non-null values.
    *
    * @param value One value
    * @param other The other value
    * @return `true` if both are not null and equal, `false` otherwise
    * @throws RuntimeException if one of the given values is `null`.
    */
  private def nullSensitiveEqual(value: Any, other: Any): Boolean = (value, other) match {
    case (_, null) | (null, _) => false
    case (a, b) => a == b
  }

  // scalastyle:off cyclomatic.complexity
  /**
    * Retrieves a [[Set]] of attributes that are covered by the given [[Filter]].
    *
    * @param filter The [[Filter]] to check for attributes.
    * @return A [[Set]] of attributes the given [[Filter]] covers.
    */
  private def attributesOf(filter: Filter): Set[String] = filter match {
    case EqualTo(attribute, _) => Set(attribute)
    case EqualNullSafe(attribute, _) => Set(attribute)
    case GreaterThan(attribute, _) => Set(attribute)
    case GreaterThanOrEqual(attribute, _) => Set(attribute)
    case LessThan(attribute, _) => Set(attribute)
    case LessThanOrEqual(attribute, _) => Set(attribute)
    case In(attribute, _) => Set(attribute)
    case IsNull(attribute) => Set(attribute)
    case IsNotNull(attribute) => Set(attribute)
    case StringStartsWith(attribute, _) => Set(attribute)
    case StringEndsWith(attribute, _) => Set(attribute)
    case StringContains(attribute, _) => Set(attribute)
    case default => childrenOf(default).flatMap(attributesOf)
  }
  // scalastyle:on cyclomatic.complexity

  /**
    * Checks if the given filter is satisfied by the given attributes.
    *
    * @param filter The [[Filter]] to check.
    * @param attributes The attributes to check for.
    * @return `true` if all the given attributes are contained in the [[Filter]], `false` otherwise.
    */
  private def areAttributesCoveredBy(filter: Filter, attributes: Seq[String]): Boolean =
    attributesOf(filter).subsetOf(attributes.toSet)

  /**
    * Retrieves the direct children of the given [[Filter]].
    *
    * @param filter The [[Filter]] to retrieve the children of.
    * @return A [[Set]] of children of the given [[Filter]].
    */
  private def childrenOf(filter: Filter): Set[Filter] = filter match {
    case p: Product => p.productIterator.collect { case f: Filter => f }.toSet
    case invalid => throw new AnalysisException(s"Cannot extract children of $invalid")
  }

  // scalastyle:off cyclomatic.complexity
  /**
    * Returns the check method for the given [[Filter]].
    *
    * @param filter The [[Filter]] to retrieve the check method for.
    * @param attributes The attributes to retrieve the check method for.
    * @return The [[Validation]].
    */
  private def toValidation(filter: Filter,
                           attributes: Seq[String]): Validation = {
    def getAttributeFilter(attribute: String)(check: Any => Boolean): Validation =
      this.getAttributeFilter(attributes, attribute)(check)
    filter match {
      case EqualTo(attribute, value) =>
        getAttributeFilter(attribute)(nullSensitiveEqual(value, _))
      case EqualNullSafe(attribute, value) =>
        getAttributeFilter(attribute)(value == _)
      case GreaterThan(attribute, value) =>
        getAttributeFilter(attribute)(compare(_, value) > 0)
      case GreaterThanOrEqual(attribute, value) =>
        getAttributeFilter(attribute)(compare(_, value) >= 0)
      case LessThan(attribute, value) =>
        getAttributeFilter(attribute)(compare(_, value) < 0)
      case LessThanOrEqual(attribute, value) =>
        getAttributeFilter(attribute)(compare(_, value) <= 0)
      case In(attribute, values) =>
        getAttributeFilter(attribute)(values.contains)
      case IsNull(attribute) =>
        getAttributeFilter(attribute)(_ == null)
      case IsNotNull(attribute) =>
        getAttributeFilter(attribute)(_ != null)
      case StringStartsWith(attribute, value) =>
        getAttributeFilter(attribute)(_.asInstanceOf[String].startsWith(value))
      case StringEndsWith(attribute, value) =>
        getAttributeFilter(attribute)(_.asInstanceOf[String].endsWith(value))
      case StringContains(attribute, value) =>
        getAttributeFilter(attribute)(_.asInstanceOf[String].contains(value))
      case Not(child) =>
        toValidation(child, attributes).andThen(!_)
      case And(left, right) =>
        (s: Seq[Any]) =>
          toValidation(left, attributes)(s) && toValidation(right, attributes)(s)
      case Or(left, right) =>
        (s: Seq[Any]) =>
          toValidation(left, attributes)(s) || toValidation(right, attributes)(s)
    }
  }
  // scalastyle: cyclomatic.complexity

  /**
    * Generates a closure that executes the given check on the field where the
    * target attribute is located at.
    *
    * @param attributes The attributes.
    * @param attribute The attribute to look for in the [[Seq]] of attributes.
    * @param check The check to execute on the target field.
    * @return A closure that executes the check on the target field.
    */
  private def getAttributeFilter(attributes: Seq[String], attribute: String)
                                (check: Any => Boolean): Validation = {
    val index = attributes.indexOf(attribute)
    if (index >= 0) {
      (s: Seq[Any]) => check(s(index))
    } else {
      (s: Seq[Any]) => true
    }
  }
}
