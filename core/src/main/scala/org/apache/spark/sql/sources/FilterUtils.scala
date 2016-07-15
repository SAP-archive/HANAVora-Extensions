package org.apache.spark.sql.sources

import org.apache.spark.sql.util.CollectionUtils.RichSeq
import org.apache.spark.sql.{AnalysisException, Row}

/**
  * Converts given filters to functions and optional remains.
  */
trait FilterUtils {
  /**
    * Validates a row-like sequence of values and returns `true` if it is valid, `false` otherwise.
    */
  type Validation = Row => Boolean

  // scalastyle:off cyclomatic.complexity
  /**
    * Transforms the attribute of the given [[Filter]] top-down.
    *
    * @param filter The [[Filter]] whose attributes to transform.
    * @param transform The transformation function to apply.
    * @return The transformed [[Filter]].
    */
  def transformFilterAttributes(filter: Filter)
                               (transform: PartialFunction[String, String]): Filter = {
    def recur(f: Filter): Filter = transformFilterAttributes(f)(transform)
    def matches(attribute: String) = transform.isDefinedAt(attribute)
    filter match {
      case e@EqualTo(attribute, _) if matches(attribute) => e.copy(transform(attribute))
      case e@EqualNullSafe(attribute, _) if matches(attribute) => e.copy(transform(attribute))
      case g@GreaterThan(attribute, _) if matches(attribute) => g.copy(transform(attribute))
      case g@GreaterThanOrEqual(attribute, _) if matches(attribute) => g.copy(transform(attribute))
      case l@LessThan(attribute, _) if matches(attribute) => l.copy(transform(attribute))
      case l@LessThanOrEqual(attribute, _) if matches(attribute) => l.copy(transform(attribute))
      case i@In(attribute, _) if matches(attribute) => i.copy(transform(attribute))
      case i@IsNull(attribute) if matches(attribute) => i.copy(transform(attribute))
      case i@IsNotNull(attribute) if matches(attribute) => i.copy(transform(attribute))
      case s@StringStartsWith(attribute, _) if matches(attribute) => s.copy(transform(attribute))
      case s@StringEndsWith(attribute, _) if matches(attribute) => s.copy(transform(attribute))
      case s@StringContains(attribute, _) if matches(attribute) => s.copy(transform(attribute))
      case n@Not(child) => n.copy(recur(child))
      case a@And(left, right) => a.copy(recur(left), recur(right))
      case o@Or(left, right) => o.copy(recur(left), recur(right))
      case default => default
    }
  }
  // scalastyle:on cyclomatic.complexity

  /**
    * Extracts [[Filter]]s of the given [[Filter]], if possible.
    *
    * A [[Filter]] can be extracted if
    * it is either a leaf [[Filter]] that any of the given attributes targets or if the filter
    * is an [[And]] and some of its children can be extracted.
    *
    * @param filter The [[Filter]] to extract from.
    * @param attributes The attributes to extract [[Filter]]s containing it.
    * @return An optional remaining [[Filter]] after extraction and an optional
    *         extracted [[Filter]].
    */
  def extractFilters(filter: Filter, attributes: Seq[String]): (Option[Filter], Option[Filter]) = {
    val children = childrenOf(filter)
    if (children.isEmpty) {
      if (areAttributesCoveredBy(filter, attributes)) {
        None -> Some(filter)
      } else {
        Some(filter) -> None
      }
    } else {
      extractMultipleChildFilters(filter, attributes)
    }
  }

  /**
    * Extracts [[Filter]]s from a [[Filter]] that has multiple children.
    *
    * @param filter The [[Filter]] to extract from.
    * @param attributes The attributes to extract [[Filter]]s containing it.
    * @return An optional remaining [[Filter]] after extraction and an optional
    *         extracted [[Filter]].
    */
  private def extractMultipleChildFilters(filter: Filter,
                                          attributes: Seq[String])
  : (Option[Filter], Option[Filter]) = filter match {
    case And(left, right) =>
      val Seq((leftRemains, leftExtracted), (rightRemains, rightExtracted)) =
        Seq(left, right).map(extractFilters(_, attributes))
      val remains =
        Seq(leftRemains, rightRemains).flatten.nonEmptyOpt.map(_.reduce(And(_, _)))
      val extracted =
        Seq(leftExtracted, rightExtracted).flatten.nonEmptyOpt.map(_.reduce(And(_, _)))
      remains -> extracted
    case default => Some(default) -> None
  }

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
  def filterToFunction(filter: Filter, attributes: Seq[String]): (Option[Filter], Validation) = {
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
      children.flatMap(filterToFunction(_, attributes)._1)(collection.breakOut)
    filter match {
      /**
        * In case one of the children of the [[And]] filter has been satisfied by the
        * given sequence of attributes, we can collapse the [[And]] to its remaining, unsatisfied
        * child. Otherwise, we will just return the original [[And]].
        */
      case and: And => Some(childResults.reduce(And(_, _))) -> validation
      /**
        * For other multi-child [[Filter]]s ([[Not]], [[Or]]) we cannot optimize and prematurely
        * extract a [[Validation]], thus we need to return an always-true [[Validation]].
        */
      case default => Some(default) -> ((r: Row) => true)
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
        (r: Row) => toValidation(left, attributes)(r) && toValidation(right, attributes)(r)
      case Or(left, right) =>
        (r: Row) => toValidation(left, attributes)(r) || toValidation(right, attributes)(r)
    }
  }
  // scalastyle: cyclomatic.complexity

  /**
    * Generates a closure that checks a target field in a row.
    *
    * The closure generated first retrieves the position of the target value in the row.
    * If the position cannot be inferred, the check will not be run since in the given set
    * there is nothing to execute the filter upon. Otherwise, the check will be given the
    * target value and executed.
    *
    * @param attributes The attributes.
    * @param attribute The attribute to look for in the [[Seq]] of attributes.
    * @param check The check to execute on the target field.
    * @return A closure that executes the check on the target field if there is a target field.
    *         Otherwise returns a closure that always returns true.
    */
  private def getAttributeFilter(attributes: Seq[String], attribute: String)
                                (check: Any => Boolean): Validation = {
    val index = attributes.indexOf(attribute)
    if (index >= 0) {
      (r: Row) => check(r(index))
    } else {
      (r: Row) => true
    }
  }
}

object FilterUtils extends FilterUtils
