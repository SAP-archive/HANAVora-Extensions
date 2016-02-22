package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types.{DecimalType, IntegerType, MetadataBuilder}
import org.scalatest.FunSuite

class FieldExtractorSuite extends FunSuite {
  val annotations = Map("bar" -> "baz", "*" -> "qux")
  val field = new Field("t", "foo", IntegerType, isNullable = false,
    annotations.foldLeft(new MetadataBuilder()) {
      case (builder, (k, v)) => builder.putString(k, v)
    }.build())
  val numericPrecision = 5
  val numericScale = 3
  val decimalField = field.copy(dataType = DecimalType(numericPrecision, numericScale))

  test("annotations with no checking for star") {
    val extractor = new FieldExtractor(0, field, checkStar = false)

    assert(extractor.annotations == annotations)
  }

  test("annotations with checking for star") {
    val extractor = new FieldExtractor(0, field, checkStar = true)

    assert(extractor.annotations == Map("bar" -> "baz"))
  }

  test("numericPrecision") {
    val extractor = new FieldExtractor(0, decimalField, checkStar = false)

    assert(extractor.numericPrecision == Some(numericPrecision))
  }

  test("numericScale") {
    val extractor = new FieldExtractor(0, decimalField, checkStar = false)

    assert(extractor.numericScale == Some(numericScale))
  }

  test("extract with no annotations") {
    val f = field.copy(metadata = new MetadataBuilder().build())
    val extractor = new FieldExtractor(0, f, checkStar = false)

    assert(extractor.extract() == Seq(
      "t" ::
        "foo" ::
        0 ::
        false ::
        "INTEGER" ::
        None ::
        None ::
        "" ::
        "" :: Nil
    ))
  }

  test("extract with annotations") {
    val extractor = new FieldExtractor(0, field, checkStar = false)

    val columnFieldsWithoutAnnotations =
      "t" ::
        "foo" ::
        0 ::
        false ::
        "INTEGER" ::
        None ::
        None :: Nil

    val expected = annotations.map {
      case (key, value) =>
        columnFieldsWithoutAnnotations :+
          key :+ value
    }.toSet

    val actual = extractor.extract().toSet

    assert(expected == actual)
  }
}
