package org.apache.spark.sql.catalyst

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.analysis.{Analyzer, Catalog}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CollectionUtils._

import scala.util.{Failure, Success, Try}

/**
  * Utility functions to fix case sensitive values and to validate them.
  */
object CaseSensitivityUtils {

  /**
    * A source of the information whether the current context is case sensitive or not.
    *
    * @tparam A The type of the source.
    */
  trait CaseSensitivitySource[A] {

    /**
      * Determines whether the given source is case sensitive or not.
      *
      * @param source The source from which a [[CaseSensitivitySource]] can be inferred.
      * @return `true` if the source is case sensitive, `false` otherwise.
      */
    def isCaseSensitive(source: A): Boolean
  }

  /** A [[CaseSensitivitySource]] of [[SQLContext]]s */
  implicit object SqlContextSource extends CaseSensitivitySource[SQLContext] {
    override def isCaseSensitive(source: SQLContext): Boolean = source.conf.caseSensitiveAnalysis
  }

  /** A [[CaseSensitivitySource]] of [[Catalog]]s */
  implicit object CatalogConfSource extends CaseSensitivitySource[Catalog] {
    override def isCaseSensitive(source: Catalog): Boolean = source.conf.caseSensitiveAnalysis
  }

  /** A [[CaseSensitivitySource]] of [[Analyzer]]s */
  implicit object AnalyzerConfSource extends CaseSensitivitySource[Analyzer] {
    override def isCaseSensitive(source: Analyzer): Boolean =
      !source.resolver.apply("a", "A")
  }

  /**
    * Converts and validates input values on basis of a given source.
    *
    * @param source The source from which a [[CaseSensitivitySource]] can be implicitly inferred.
    * @tparam A The type of the source.
    */
  implicit class CaseSensitivityConverter[A: CaseSensitivitySource](source: A) {

    /**
      * Fixes the casing of the given [[TableIdentifier]].
      *
      * @param id The [[TableIdentifier]] to fix the casing of.
      * @return The [[TableIdentifier]] with both the `.table` and `.database` part lower-cased if
      *         the source was case insensitive, otherwise the [[TableIdentifier]] itself.
      */
    def fixCase(id: TableIdentifier): TableIdentifier =
      TableIdentifier(fixCase(id.table), id.database.map(fixCase))

    /**
      * Fixes the casing of the given [[String]].
      *
      * @param string The [[String]] to fix the casing of.
      * @return The lower-cased string if the source was case sensitive, otherwise
      *         the string itself.
      */
    def fixCase(string: String): String =
      if (implicitly[CaseSensitivitySource[A]].isCaseSensitive(source)) {
        string
      } else {
        string.toLowerCase
      }

    private def fixCase(structField: StructField): StructField =
      structField.copy(fixCase(structField.name))

    private def fixCase(structType: StructType): StructType =
      StructType(structType.map(fixCase))

    /**
      * Checks if the given [[StructType]] is valid with the current settings and returns a
      * sane version of it, if valid.
      *
      * @param schema The [[StructType]] to check.
      * @return [[Success]] of the given [[StructType]] with the current casing if
      *         it had no duplicate fields within that casing, otherwise a [[Failure]]
      *         with a [[DuplicateFieldsException]] that states the duplicate fields.
      */
    def validatedSchema(schema: StructType): Try[StructType] = {
      val fixedSchema = fixCase(schema)
      val duplicates = fixedSchema.map(_.name).duplicates
      if (duplicates.nonEmpty) {
        Failure(DuplicateFieldsException(schema, fixedSchema, duplicates))
      } else {
        Success(fixedSchema)
      }
    }

    /**
      * Checks if the given [[StructType]] has duplicate fields with the current case sensitivity.
      *
      * @param schema The [[StructType]] to check.
      * @throws DuplicateFieldsException if the given [[StructType]] has duplicate fields
      *                                  with the current case sensitivity settings.
      */
    def validateSchema(schema: StructType): Unit =
      validatedSchema(schema).get
  }

  /**
    * An exception if there are duplicate fields in a given [[StructType]].
    *
    * @param originalSchema The original schema.
    * @param schema The schema after applying case sensitivity rules.
    * @param duplicateFields The duplicate fields.
    */
  case class DuplicateFieldsException(
      originalSchema: StructType,
      schema: StructType,
      duplicateFields: Set[String])
    extends RuntimeException(
      s"""Given schema contains duplicate fields after applying case sensitivity rules:
         |${duplicateFields.mkString(", ")}
         |Given schema:
         |$originalSchema
         |After applying case sensitivity rules:
         |$schema
       """.stripMargin)
}
