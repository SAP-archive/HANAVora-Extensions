package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class FieldExtractorSuite extends FunSuite {
  val numeric = new {
    val precisionOf = new {
      val decimal = 5
      val int = 32
      val long = 64
      val double = 53
      val float = 24
    }
    val radixOf = new {
      val decimal = 10
      val int = 2
      val long = 2
      val float = 2
      val double = 2
    }
    val scaleOf = new {
      val decimal = 3
      val int = 0
      val long = 0
      val float = null
      val double = null
    }
  }

  val annotations = Map("bar" -> "baz", "*" -> "qux")

  val fields = new {
    val int = new Field("t", "foo", IntegerType, isNullable = false,
      annotations.foldLeft(new MetadataBuilder()) {
        case (builder, (k, v)) => builder.putString(k, v)
      }.build())
    val decimal = int.copy(dataType =
      DecimalType(numeric.precisionOf.decimal, numeric.scaleOf.decimal))
    val double = int.copy(dataType = DoubleType)
    val float = int.copy(dataType = FloatType)
    val long = int.copy(dataType = LongType)
  }

  def noStarExtractor(f: Field): FieldExtractor =
    new FieldExtractor(0, f, checkStar = false)


  test("annotations with no checking for star") {
    val extractor = new FieldExtractor(0, fields.int, checkStar = false)

    assert(extractor.annotations == annotations)
  }

  test("annotations with checking for star") {
    val extractor = new FieldExtractor(0, fields.int, checkStar = true)

    assert(extractor.annotations == Map("bar" -> "baz"))
  }

  test("numericPrecision") {
    assertResult(Option(numeric.precisionOf.double)) {
      noStarExtractor(fields.double).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.decimal)) {
      noStarExtractor(fields.decimal).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.int)) {
      noStarExtractor(fields.int).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.float)) {
      noStarExtractor(fields.float).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.long)){
      noStarExtractor(fields.long).numericPrecision
    }
  }

  test("numericScale") {
    assertResult(Option(numeric.scaleOf.double)){
      noStarExtractor(fields.double).numericScale
    }
    assertResult(Option(numeric.scaleOf.decimal)){
      noStarExtractor(fields.decimal).numericScale
    }
    assertResult(Option(numeric.scaleOf.int)){
      noStarExtractor(fields.int).numericScale
    }
    assertResult(Option(numeric.scaleOf.float)){
      noStarExtractor(fields.float).numericScale
    }
    assertResult(Option(numeric.scaleOf.long)){
      noStarExtractor(fields.long).numericScale
    }
  }

  test("numericPrecisionRadix") {
    assertResult(Option(numeric.radixOf.double)){
      noStarExtractor(fields.double).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.decimal)){
      noStarExtractor(fields.decimal).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.int)){
      noStarExtractor(fields.int).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.float)){
      noStarExtractor(fields.float).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.long)){
      noStarExtractor(fields.long).numericPrecisionRadix
    }
  }

  test("extract with no annotations") {
    val f = fields.int.copy(metadata = new MetadataBuilder().build())
    val extractor = new FieldExtractor(0, f, checkStar = false)

    assert(extractor.extract() == Seq(
      "t" ::
        "foo" ::
        0 ::
        false ::
        "INTEGER" ::
        Some(numeric.precisionOf.int) ::
        Some(numeric.radixOf.int) ::
        Some(numeric.scaleOf.int) ::
        None ::
        None :: Nil
    ))
  }

  test("extract with annotations") {
    val extractor = new FieldExtractor(0, fields.int, checkStar = false)

    val columnFieldsWithoutAnnotations =
      "t" ::
        "foo" ::
        0 ::
        false ::
        "INTEGER" ::
        Some(numeric.precisionOf.int) ::
        Some(numeric.radixOf.int) ::
        Some(numeric.scaleOf.int) :: Nil

    val expected = annotations.map {
      case (key, value) =>
        columnFieldsWithoutAnnotations :+
          key :+ value
    }.toSet

    val actual = extractor.extract().toSet

    assert(expected == actual)
  }
}
