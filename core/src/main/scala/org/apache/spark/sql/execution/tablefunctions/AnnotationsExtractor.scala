package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.types.{Metadata, MetadataAccessor}

/**
  * Extracts annotations and respects if it should filter on stars or not.
  */
case class AnnotationsExtractor(metadata: Map[String, Any], checkStar: Boolean) {
  lazy val annotations: Map[String, String] =
    metadata
      .filter {
        case (k, v) if checkStar => k != "*"
        case _ => true
      }
      .mapValues(_.toString)
}

object AnnotationsExtractor {
  def apply(metadata: Metadata, checkStar: Boolean): AnnotationsExtractor =
    AnnotationsExtractor(MetadataAccessor.metadataToMap(metadata), checkStar)
}
