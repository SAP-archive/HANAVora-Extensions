package org.apache.spark.sql.util

import scala.collection.immutable.Queue

object CollectionUtil {
  implicit class RichSeq[A](val seq: Seq[A]) {
    def orderPreservingDistinct: Queue[A] = {
      def inner(seen: Set[A], acc: Queue[A], remaining: Seq[A]): Queue[A] = remaining match {
        case coll if coll.isEmpty =>
          acc
        case coll if !seen.contains(coll.head) =>
          inner(seen + coll.head, acc.enqueue(coll.head), coll.tail)
        case _ =>
          inner(seen, acc, remaining.tail)
      }

      inner(Set.empty, Queue.empty, seq)
    }
  }
}
