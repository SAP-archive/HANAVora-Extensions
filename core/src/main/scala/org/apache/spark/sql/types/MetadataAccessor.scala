package org.apache.spark.sql.types

import org.apache.spark.sql.catalyst.expressions.{AnnotationReference, Literal, Expression}
import org.apache.spark.unsafe.types.UTF8String

/**
 * This class makes it possible to retrieve meta data
 * information from [[Metadata]] object.
 */
private[sql] object MetadataAccessor {

  def nonEmpty(metadata: Metadata): Boolean = {
    metadata.map.nonEmpty
  }

  def isEmpty(metadata: Metadata): Boolean = {
    metadata.map.isEmpty
  }

  def mapToMetadata(map: Map[String, Any]): Metadata = {
    val res = new MetadataBuilder()
    map.foreach ({
      case (k, v:Long) => res.putLong(k, v)
      case (k, v:Double) => res.putDouble(k, v)
      case (k, v:String) => res.putString(k, v)
      case (k, v:Array[String]) => res.putStringArray(k, v)
    })
    res.build()
  }

  def metadataToMap(metadata:Metadata): Map[String, Any] = {
    metadata.map
  }

  def expressionMapToMetadata(metadataMap: Map[String, Expression]): Metadata = {
    val res = new MetadataBuilder()
    metadataMap.foreach {
      case (k, v:Literal) =>
        v.dataType match {
          case StringType => res.putString(k, v.value.asInstanceOf[UTF8String].toString())
          case LongType => res.putLong(k, v.value.asInstanceOf[Long])
          case DoubleType => res.putDouble(k, v.value.asInstanceOf[Double])
          case NullType => res.putString(k, null)
        }
      case (k, v:AnnotationReference) =>
        sys.error("column metadata can not have a reference to another column metadata")
    }
    res.build()
  }

  /**
    * Propagates metadata from ''older'' metadata to ''newer'' metadata.
    *
    * If the ''newer'' metadata contains a pair like this: ('*' -> v) then
    * the ''older'' metadata will be propagated with the value 'v'.
    *
    * @param older The older metadata.
    * @param newer The newer metadata.
    * @return a new [[Metadata]] object containing the propagated metadata from ''older'' to
    *         ''newer''.
    */
  def propagateMetadata(older: Metadata, newer: Metadata): Metadata = {
    val olderMap = metadataToMap(older)
    val newerMap = metadataToMap(newer)

    val result = newerMap.get("*") match {
      case Some(v) if newerMap.size > 1 =>
        sys.error(s"newer metadata contains '*' and other entries which is invalid: $newer")
      case Some(v) =>
        mapToMetadata(olderMap.keySet.union(newerMap.keySet).map(k => (k, v)).toMap)
      case None =>
        mapToMetadata(newerMap ++ (olderMap -- newerMap.keySet.intersect(olderMap.keySet)))
    }
    result
  }

  /**
    * Filters metadata according to a set of filters.
    *
    * A filter can be either:
    *   - a key identifier, e.g. 'foo'.
    *   - a '*' which indicates that item in the metadata is accepted.
    *
    * @param metadata The metadata object we want to filter.
    * @return The filtered metadata.
    */
  def filterMetadata(metadata: Metadata, filters: Set[String]): Metadata = {
      mapToMetadata(
        metadataToMap(metadata)
          .filter(p => filters.contains(p._1) || filters.contains("*")))
  }
}
