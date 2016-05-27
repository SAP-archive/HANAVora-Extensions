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

  type Field = (String, DataType, Boolean, Metadata)

  val fields = new {
    val int: Field = ("foo", IntegerType, false,
      annotations.foldLeft(new MetadataBuilder()) {
        case (builder, (k, v)) => builder.putString(k, v)
      }.build())
    val decimal = int.copy(_2 =
      DecimalType(numeric.precisionOf.decimal, numeric.scaleOf.decimal))
    val double = int.copy(_2 = DoubleType)
    val float = int.copy(_2 = FloatType)
    val long = int.copy(_2 = LongType)
  }

  def createExtractor(field: Field)(checkStar: Boolean): FieldExtractor = {
    val (name, dataType, isNullable, metadata) = field
    new FieldExtractor(0, "f", name, dataType, metadata, isNullable, checkStar)
  }

  test("annotations with no checking for star") {
    val extractor = createExtractor(fields.int)(checkStar = false)

    assert(extractor.annotations == annotations)
  }

  test("annotations with checking for star") {
    val extractor = createExtractor(fields.int)(checkStar = true)

    assert(extractor.annotations == Map("bar" -> "baz"))
  }

  test("numericPrecision") {
    assertResult(Option(numeric.precisionOf.double)) {
      createExtractor(fields.double)(checkStar = false).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.decimal)) {
      createExtractor(fields.decimal)(checkStar = false).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.int)) {
      createExtractor(fields.int)(checkStar = false).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.float)) {
      createExtractor(fields.float)(checkStar = false).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.long)){
      createExtractor(fields.long)(checkStar = false).numericPrecision
    }
  }

  test("numericScale") {
    assertResult(Option(numeric.scaleOf.double)){
      createExtractor(fields.double)(checkStar = false).numericScale
    }
    assertResult(Option(numeric.scaleOf.decimal)){
      createExtractor(fields.decimal)(checkStar = false).numericScale
    }
    assertResult(Option(numeric.scaleOf.int)){
      createExtractor(fields.int)(checkStar = false).numericScale
    }
    assertResult(Option(numeric.scaleOf.float)){
      createExtractor(fields.float)(checkStar = false).numericScale
    }
    assertResult(Option(numeric.scaleOf.long)){
      createExtractor(fields.long)(checkStar = false).numericScale
    }
  }

  test("numericPrecisionRadix") {
    assertResult(Option(numeric.radixOf.double)){
      createExtractor(fields.double)(checkStar = false).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.decimal)){
      createExtractor(fields.decimal)(checkStar = false).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.int)){
      createExtractor(fields.int)(checkStar = false).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.float)){
      createExtractor(fields.float)(checkStar = false).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.long)){
      createExtractor(fields.long)(checkStar = false).numericPrecisionRadix
    }
  }
}
