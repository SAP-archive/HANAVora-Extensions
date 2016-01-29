package org.apache.spark.sql.types

import java.io._

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.AllScalaRegistrar
import org.scalatest.FunSuite

class NodeSuite extends FunSuite {

  def isSerializable(input: Any): Boolean = {
    val byteArray = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(byteArray)
    oos.writeObject(input)
    oos.close()
    val ois = new ObjectInputStream(new ByteArrayInputStream(byteArray.toByteArray))
    val output = ois.readObject()
    ois.close()
    input == output
  }

  def isKryoSerializable(input: Any): Boolean = {
    val kryo = new Kryo()
    kryo.setRegistrationRequired(false)
    new AllScalaRegistrar().apply(kryo)
    kryo.register(input.getClass)
    kryo.setReferences(false)
    kryo.setClassLoader(input.getClass.getClassLoader)
    val byteArray = new ByteArrayOutputStream()
    val kryoOutput = new Output(byteArray)
    kryo.writeObject(kryoOutput, input)
    kryoOutput.flush()
    val kryoInput = new Input(new ByteArrayInputStream(byteArray.toByteArray))
    val output = kryo.readObject(kryoInput, input.getClass)
    kryoInput.close()
    kryoOutput.close()
    input == output
  }

  test("Node is serializable") {
    assert(isSerializable(Node(Seq(1), IntegerType)))
    assert(isSerializable(Node(Seq(1L), LongType)))
    assert(isSerializable(Node(Seq("1"), StringType)))
    assert(isSerializable(Node(Seq(1, 2), IntegerType)))
    assert(isSerializable(Node(Seq(1L, 2L), LongType)))
    // assert(isSerializable(Node(Seq("1", "2", 1))))
    assert(isSerializable(Node(Seq(1), IntegerType, 1, 1, true)))
  }

  test("Node is serializable with Kryo") {
    assert(isKryoSerializable(Node(Seq(1), IntegerType)))
    assert(isKryoSerializable(Node(Seq(1L), LongType)))
    assert(isKryoSerializable(Node(Seq("1"), StringType)))
    assert(isKryoSerializable(Node(Seq(1, 2), IntegerType)))
    assert(isKryoSerializable(Node(Seq(1L, 2L), LongType)))
    // assert(isKryoSerializable(Node(Seq("1", "2", 1))))
    assert(isKryoSerializable(Node(Seq(1), IntegerType, 1, 1, true)))
  }

}
