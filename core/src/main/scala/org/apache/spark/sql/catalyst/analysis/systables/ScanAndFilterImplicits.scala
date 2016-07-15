package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.{And, Filter, FilterUtils}
import org.apache.spark.sql.types.StructType
import FilterUtils._

/** Implicits for the filter to [[Validation]] conversion */
trait ScanAndFilterImplicits {
  implicit class RichFilter(val filter: Filter) {
    /**
      * Calls `FilterUtils.extractFilters` on the given [[Filter]].
      *
      * Splits the given [[Filter]] into optional remaining and optional extracted [[Filter]] based
      * on the given attributes.
      *
      * @param attributes The attributes.
      * @return The optional remaining [[Filter]] and the optional extracted [[Filter]].
      */
    def extract(attributes: Seq[String]): (Option[Filter], Option[Filter]) =
      extractFilters(filter, attributes)

    /**
      * Creates a [[Validation]] from the given [[Filter]] with the given attributes.
      *
      * @param attributes The attributes to validate.
      * @return A [[Validation]] for the given [[Filter]] and attributes and the remaining
      *         [[Filter]].
      */
    def toValidation(attributes: Seq[String]): (Option[Filter], Validation) =
      filterToFunction(filter, attributes)

    /**
      * Transforms the attributes of the given [[Filter]] top-down.
      *
      * @param transform The transformation function.
      * @return The transformed [[Filter]].
      */
    def transformAttributes(transform: PartialFunction[String, String]): Filter =
      transformFilterAttributes(filter)(transform)
  }

  implicit class RichFilterSeq(val filterSeq: Seq[Filter]) {
    /**
      * Constructs one [[Validation]] from the given [[Filter]]s with the required attributes.
      *
      * @param attributes The attributes to check.
      * @return A [[Validation]] constructed from all the [[Filter]]s as well as the remaining
      *         [[Filter]]s.
      */
    def toValidation(attributes: Seq[String]): (Seq[Filter], Validation) = {
      val (remainingFilters, validations) = filterSeq.map(filterToFunction(_, attributes)).unzip
      remainingFilters.flatten -> validations.merge
    }

    /**
      * Merges the given [[Seq]] of [[Filter]]s to one [[Filter]] by [[And]]ing them.
      *
      * @return [[Some]] merged [[Filter]] if the sequence was non-empty, [[None]] otherwise.
      */
    def merge: Option[Filter] =
      if (filterSeq.nonEmpty) Some(filterSeq.reduce(And(_, _))) else None
  }

  implicit class RichValidationSeq(val validationSeq: Seq[Validation]) {
    /**
      * Merges the given [[Seq]] of [[Validation]]s to a single [[Validation]].
      *
      * @return A [[Validation]] that `&&`s all given [[Validation]]s.
      */
    def merge: Validation = (r: Row) => validationSeq.forall(_.apply(r))
  }

  implicit class RichStructType(val structType: StructType) {

    /**
      * Constructs the function to scan required columns of a result set.
      *
      * @param requiredColumns The required columns.
      * @return A function to discard all non-required columns.
      */
    def buildScan(requiredColumns: Seq[String]): Row => Row = {
      val columnNames = structType.map(_.name)
      val columnNamesSet = columnNames.toSet
      val requiredSet = requiredColumns.toSet
      val difference = requiredSet.diff(columnNamesSet)
      require(
        difference.isEmpty,
        message =
          s"""Columns required that are not present in the absolute column set:
              |Absolute: ${columnNames.mkString(",")}
              |Required: ${requiredColumns.mkString(", ")}
              |Difference: ${difference.mkString(", ")}
        """.stripMargin)
      val columnUsages = requiredColumns.map(column => columnNames.indexWhere(_ == column))
      (r: Row) => Row.fromSeq(columnUsages.map(r.apply))
    }

    /**
      * Picks the required columns from the given values, filters and returns the result.
      *
      * @param requiredColumns The required columns.
      * @param filters The [[Filter]]s.
      * @return A function that scans and filters a [[Seq]] of [[Row]]s.
      */
    def buildPrunedFilteredScan(requiredColumns: Seq[String],
                                filters: Seq[Filter]): Seq[Row] => Seq[Row] = {
      /** Build scan and validation functions. */
      val scanFunction = buildScan(requiredColumns)
      val (remaining, validation) = filters.toValidation(requiredColumns)
      assert(remaining.isEmpty, message = s"Filters left unprocessed: ${remaining.mkString(", ")}")

      (values: Seq[Row]) =>
        /** Scan the result and filter it. */
        values.foldLeft(Seq.empty[Row]) {
          case (acc, value) =>
            val scanned = scanFunction(value)
            if (validation(scanned)) {
              acc :+ scanned
            } else acc
        }
    }
  }

}

object ScanAndFilterImplicits extends ScanAndFilterImplicits
