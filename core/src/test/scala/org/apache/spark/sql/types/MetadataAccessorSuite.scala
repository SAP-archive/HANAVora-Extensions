package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.{Literal, Expression}
import org.scalatest.FunSuite

/**
  * A set of unit tests of the [[MetadataAccessor]] class.
  */
// scalastyle:off magic.number
class MetadataAccessorSuite extends FunSuite {

  test("expression map is written correctly to Metadata") {
    val expressionMap = Map[String, Expression] (
      "stringKey" -> Literal.create("stringValue", StringType),
      "longKey" -> Literal.create(10L, LongType),
      "doubleKey" -> Literal.create(1.234, DoubleType),
      "nullKey" -> Literal.create(null, NullType)
    )
    val actual = MetadataAccessor.expressionMapToMetadata(expressionMap)

    assertResult("stringValue")(actual.getString("stringKey"))
    assertResult(10)(actual.getLong("longKey"))
    assertResult(1.234)(actual.getDouble("doubleKey"))
    assertResult(null)(actual.getString("nullKey"))
  }

  test("metadata propagation works correctly") {
    val oldMetadata = new MetadataBuilder()
      .putString("key1", "value1")
      .putString("key2", "value2")
      .putLong("key3", 10L)
      .build()

    val newMetadata = new MetadataBuilder()
      .putString("key1", "overriden")
      .putString("key4", "value4")
      .build()

    val expected = new MetadataBuilder()
      .putString("key1", "overriden")
      .putString("key2", "value2")
      .putLong("key3", 10L)
      .putString("key4", "value4")
      .build()

    val actual = MetadataAccessor.propagateMetadata(oldMetadata, newMetadata)

    assertResult(expected)(actual)
  }

  test("filter metadata works correctly") {
    val metadata = new MetadataBuilder()
      .putString("key1", "value1")
      .putString("key2", "value2")
      .putLong("key3", 10L)
      .build()

    val expected1 = new MetadataBuilder()
      .putString("key1", "value1")
      .build()

    assertResult(expected1)(MetadataAccessor.filterMetadata(metadata, ("key1" :: Nil).toSet))

    assertResult(metadata)(MetadataAccessor.filterMetadata(metadata, ("*" :: Nil).toSet))
  }
}
