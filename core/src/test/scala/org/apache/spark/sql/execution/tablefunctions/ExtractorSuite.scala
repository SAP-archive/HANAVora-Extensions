package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types._
import org.scalatest.FunSuite

class ExtractorSuite extends FunSuite {
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

  val metadata = annotations.foldLeft(new MetadataBuilder()) {
    case (builder, (k, v)) => builder.putString(k, v)
  }.build()

  val types = new {
    val int = IntegerType
    val decimal = DecimalType(numeric.precisionOf.decimal, numeric.scaleOf.decimal)
    val double = DoubleType
    val float = FloatType
    val long = LongType
  }

  test("annotations with no checking for star") {
    val extractor = new AnnotationsExtractor(metadata, checkStar = false)

    assert(extractor.annotations == annotations)
  }

  test("annotations with checking for star") {
    val extractor = new AnnotationsExtractor(metadata, checkStar = true)

    assert(extractor.annotations == annotations - "*")
  }

  test("numericPrecision") {
    assertResult(Option(numeric.precisionOf.double)) {
      new DataTypeExtractor(types.double).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.decimal)) {
      new DataTypeExtractor(types.decimal).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.int)) {
      new DataTypeExtractor(types.int).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.float)) {
      new DataTypeExtractor(types.float).numericPrecision
    }
    assertResult(Option(numeric.precisionOf.long)){
      new DataTypeExtractor(types.long).numericPrecision
    }
  }

  test("numericScale") {
    assertResult(Option(numeric.scaleOf.double)){
      new DataTypeExtractor(types.double).numericScale
    }
    assertResult(Option(numeric.scaleOf.decimal)){
      new DataTypeExtractor(types.decimal).numericScale
    }
    assertResult(Option(numeric.scaleOf.int)){
      new DataTypeExtractor(types.int).numericScale
    }
    assertResult(Option(numeric.scaleOf.float)){
      new DataTypeExtractor(types.float).numericScale
    }
    assertResult(Option(numeric.scaleOf.long)){
      new DataTypeExtractor(types.long).numericScale
    }
  }

  test("numericPrecisionRadix") {
    assertResult(Option(numeric.radixOf.double)){
      new DataTypeExtractor(types.double).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.decimal)){
      new DataTypeExtractor(types.decimal).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.int)){
      new DataTypeExtractor(types.int).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.float)){
      new DataTypeExtractor(types.float).numericPrecisionRadix
    }
    assertResult(Option(numeric.radixOf.long)){
      new DataTypeExtractor(types.long).numericPrecisionRadix
    }
  }
}
