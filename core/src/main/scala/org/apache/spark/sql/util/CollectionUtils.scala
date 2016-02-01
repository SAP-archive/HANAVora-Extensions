package org.apache.spark.sql.util

/**
  * a class the defines extra useful methods on Scala collections.
 */

object CollectionUtils {

  implicit class MapOps[K, V](map: Map[K, V]) {

    /**
      * If the maps contains the key ''key'' it returns the same map. Otherwise
      * a new map is created from the old map with the new ''key'' ''value'' pair.
      * @param key The key.
      * @param value The value.
      */
    def putIfAbsent(key: K, value: V): Map[K, V] = {
      if (map.contains(key)) {
        map
      } else {
        map + (key -> value)
      }
    }

  }
}


