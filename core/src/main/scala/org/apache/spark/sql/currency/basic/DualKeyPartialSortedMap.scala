package org.apache.spark.sql.currency.basic

import java.util

import scala.collection.mutable

/**
  * A dual key map implemented as a nested map.
  * The first key is treated as an unordered entity, the
  * second key is assumed to have a natural order.
  * This allows to search for values where the second key is not present,
  * but the next lower key is used instead.
  *
  * The implementation is using a nested `HashMap[TreeMap]`. By this,
  * the runtime complexity of inserts (`put`) and lookups (`getSortedKeyFloorValue`)
  * is `O(N log M)`, where `N` is the number of entries in the map,
  * and `M` is the average number of values having the same unordered key.
  *
  * TODO(CS,MD): Candidate for hanalite-scala-commons
  *
  * @tparam U: The type of the unordered key
  * @tparam S: The type of the ordered key
  * @tparam V: The type of the value
  */
class DualKeyPartialSortedMap[U, S, V](implicit convertToOrdered: S => Ordered[S])
  extends Serializable {
  private val map = new mutable.HashMap[U, util.TreeMap[S, V]]()

  /**
    * Gets the value of the given dual `(unordered, ordered)` key.
    * If the key does not exist, the next lower ordered key will be used, if possible.
    * Else, [[None]] is returned.
    *
    * @param unorderedKey The unordered key
    * @param orderedKey The ordered key. If the key does not exist,
    *                   the next lower ordered key will be used (if possible),
    *                   else, [[None]] is returned.
    * @return The value of the given dual `(unordered, ordered)` key
    */
  def getSortedKeyFloorValue(unorderedKey: U, orderedKey: S): Option[V] = {
    map.get(unorderedKey) match {
      case Some(sortedMap) =>
        sortedMap.floorEntry(orderedKey) match {
          case null => None
          case entry: util.Map.Entry[S, V] => Some(entry.getValue)
        }
      case None => None
    }
  }

  /**
    * Adds a new dual key entry to the map.
    *
    * @param unorderedKey The unordered key part
    * @param orderedKey The ordered key part
    * @param value The value
    */
  def put(unorderedKey: U, orderedKey: S, value: V): Unit =
    map.getOrElseUpdate(unorderedKey, new util.TreeMap[S, V]())
      .put(orderedKey, value)

  /**
    * Removes all entries from the map.
    */
  def clear(): Unit = map.clear()
}
