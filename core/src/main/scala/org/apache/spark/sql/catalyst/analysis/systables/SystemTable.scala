package org.apache.spark.sql.catalyst.analysis.systables

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.systables.filterToFunction.Validation
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.{Row, SQLContext}

/**
  * A system table.
  */
trait SystemTable extends BaseRelation with PrunedFilteredScan {

  /**
    * A handle to the SQL Context that was used to create this plan. Since many operators need
    * access to the sqlContext for RDD operations or configuration this field is automatically
    * populated by the query planning infrastructure.
    */
  @transient
  val sqlContext: SQLContext

  def sparkContext: SparkContext = sqlContext.sparkContext
}

/**
  * Utility trait to scan and filter in memory.
  */
trait ScanAndFilterUtility {
  this: SystemTable =>

  /**
    * Merges the given [[Seq]] of [[Validation]]s to a single [[Validation]].
    *
    * @param validations The [[Seq]] of [[Validation]]s.
    * @return A [[Validation]] that `&&`s all given [[Validation]]s.
    */
  protected def mergeValidations(validations: Seq[Validation]): Validation =
    (s: Seq[Any]) => validations.forall(_(s))

  /**
    * Picks the required columns from the given values, filters and returns the result.
    *
    * @param requiredColumns The required columns.
    * @param filters The [[Filter]]s.
    * @param values The given values to scan and filter.
    * @return The scanned and filtered values.
    */
  protected def scanAndValidate(requiredColumns: Seq[String],
                                filters: Seq[Filter],
                                values: Seq[Row]): Seq[Row] = {
    /** Build scan and validation functions. */
    val scanFunction = buildScan(requiredColumns)
    val (remaining, validation) = buildValidation(requiredColumns, filters)
    assert(remaining.isEmpty, message = s"Filters left unprocessed: ${remaining.mkString(", ")}")

    /** Scan the result, filter it and convert it to an RDD */
    values.foldLeft(Seq.empty[Row]) {
      case (acc, value) =>
        val scanned = scanFunction(value.toSeq)
        if (validation(scanned)) {
          acc :+ Row.fromSeq(scanned)
        } else acc
    }
  }

  /**
    * Constructs [[Validation]] from the given [[Filter]]s with the required attributes.
    *
    * @param attributes The attributes to check.
    * @param filters The [[Filter]]s to convert to [[Validation]].
    * @return A [[Validation]] constructed from all the [[Filter]]s.
    */
  protected def buildValidation(attributes: Seq[String],
                                filters: Seq[Filter]): (Seq[Filter], Validation) = {
    val (remainingFilters, validations) = filters.map(filterToFunction(_, attributes)).unzip
    remainingFilters.flatten -> mergeValidations(validations)
  }

  /**
    * Constructs the function to scan required colums of a result set.
    *
    * @param requiredColumns The required columns.
    * @return A function to discard all non-required columns.
    */
  protected def buildScan(requiredColumns: Seq[String]): Seq[Any] => Seq[Any] = {
    val columnNames = schema.map(_.name)
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
    (s: Seq[Any]) => columnUsages.map(s.apply)
  }
}

/**
  * A [[SystemTable]] that is capable of filtering and scanning by itself.
  */
trait AutoScan extends ScanAndFilterUtility {
  this: SystemTable =>

  /** @inheritdoc */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    val values = execute()
    val result = scanAndValidate(requiredColumns, filters, values)
    sparkContext.parallelize(result)
  }

  /**
    * Executes the functionality of this table.
    *
    * @return The result of this system table.
    */
  def execute(): Seq[Row]
}

