package org.apache.spark.sql.types

import java.io._

import org.scalatest.FunSuite

class NodeSuite extends FunSuite {

  def isSerializable[T](input : T) : Boolean = {
    val byteArray = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(byteArray)
    oos.writeObject(input)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(byteArray.toByteArray))
    val output = ois.readObject()
    input == output
  }

  test("Node is serializable") {
    /* UNSUPPORTED: assert(isSerializable(Node(Seq()))) */
    assert(isSerializable(Node(Seq(1))))
    assert(isSerializable(Node(Seq(1L))))
    assert(isSerializable(Node(Seq("1"))))
    assert(isSerializable(Node(Seq(1, 2))))
    assert(isSerializable(Node(Seq(1L, 2L))))
    assert(isSerializable(Node(Seq("1", "2", 1))))
  }

}
