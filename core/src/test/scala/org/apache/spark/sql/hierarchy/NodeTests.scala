package org.apache.spark.sql.hierarchy

import org.apache.spark.Logging
import org.apache.spark.sql.types.{Node, NodeHelpers, StringType}

import scala.collection.mutable.ArrayBuffer

// scalastyle:off magic.number
// scalastyle:off file.size.limit
class NodeTests extends NodeUnitTestSpec with Logging {
    var nodes = ArrayBuffer[Node]()
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 1L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 1L, 2L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 1L, 3L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 2L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 3L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 4L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 4L, 1L))
    nodes += Node(path = null, pathDataType = StringType, ordPath = List(1L, 4L, 2L))
  log.info("Running unit tests for sorting class Node\n")
  nodes.toArray should equal {
    // deterministic generator:
    val myRand = new scala.util.Random(42)

    // take copy of array-buffer, shuffle it
    val shuffled_nodes = myRand.shuffle(nodes.toSeq)

    // shuffled?:
    shuffled_nodes should not equal nodes.toArray

    shuffled_nodes.sorted(NodeHelpers.OrderedNode)
  }
  log.info("Testing function compareToRecursive\n")
  val x = Node(null, null)

  0 should equal {x.compareToRecursive(Seq(), Seq())}
  0 should be > {x.compareToRecursive(Seq(), Seq(1))}
  0 should be < {x.compareToRecursive(Seq(1), Seq())}
  0 should equal {x.compareToRecursive(Seq(1,2), Seq(1,2))}
  0 should be < {x.compareToRecursive(Seq(1,2), Seq(1))}
  0 should be > {x.compareToRecursive(Seq(1), Seq(1,2))}

}
// scalastyle:on magic.number
// scalastyle:on file.size.limit
