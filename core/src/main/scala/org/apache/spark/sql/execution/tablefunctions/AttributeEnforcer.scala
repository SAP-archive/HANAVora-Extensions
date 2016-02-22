package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, StringType}
import org.apache.spark.sql.util.GenericUtil._

/** A collection of
  * [[org.apache.spark.sql.execution.tablefunctions.DataTypeEnforcers.DataTypeEnforcer]]s
  * for convenient lookup */
private object DataTypeEnforcers {
  sealed abstract class DataTypeEnforcer[A] {
    /** Enforces that the given value corresponds to type [[A]]
      *
      * @param value The value to check
      * @return The value casted to [[A]]
      * @throws AssertionError if the value does not match the expected type
      */
    def enforce(value: Any): A

    /** Checks if the enforcer can enforce the given [[org.apache.spark.sql.types.DataType]]
      *
      * @param dataType The datatype to check
      * @return True if it can be enforced by this enforcer, false otherwise
      */
    def matches(dataType: DataType): Boolean
  }

  /** Type enforcer that associates a [[org.apache.spark.sql.types.DataType]] with a type [[A]]
    *
    * @param dataType The data type to enforce
    * @tparam A The type that is the output of the enforcement
    */
  sealed abstract class FixedTypeEnforcer[A](val dataType: DataType) extends DataTypeEnforcer[A] {
    final override def matches(dataType: DataType): Boolean = this.dataType == dataType
    final def enforce(value: Any): A = value.matchOptional(matchType) match {
      case None =>
        throw new AssertionError("Value not of asserted type")

      case Some(v) => v
    }
    protected def matchType: PartialFunction[Any, A]
  }

  /** Type enforcer for [[org.apache.spark.sql.types.IntegerType]] */
  object IntegerTypeEnforcer extends FixedTypeEnforcer[Int](IntegerType) {
    override def matchType: PartialFunction[Any, Int] = {
      case i: Int => i
    }
  }

  /** Type enforcer for [[org.apache.spark.sql.types.BooleanType]] */
  object BooleanTypeEnforcer extends FixedTypeEnforcer[Boolean](BooleanType) {
    override def matchType: PartialFunction[Any, Boolean] = {
      case b: Boolean => b
    }
  }

  /** Type enforcer for [[org.apache.spark.sql.types.StringType]] */
  object StringTypeEnforcer extends FixedTypeEnforcer[String](StringType) {
    override protected def matchType: PartialFunction[Any, String] = {
      case s => s.toString
    }
  }

  /** List of current enforcers */
  // TODO (YH, AC) write futher enforcers for other data types
  private val enforcers = IntegerTypeEnforcer :: BooleanTypeEnforcer :: StringTypeEnforcer :: Nil

  /** Searches for an enforcer that matches the given [[org.apache.spark.sql.types.DataType]]
    *
    * @param dataType The [[DataType]] for which an enforcer shall be found
    * @return [[Option]] with a possible enforcer, if found
    */
  def enforcerFor(dataType: DataType): Option[DataTypeEnforcer[_]] = {
    enforcers.find(_.matches(dataType)).asInstanceOf[Option[DataTypeEnforcer[_]]]
  }
}

/** Enforces the properties and [[org.apache.spark.sql.types.DataType]] of an attribute on values.
  *
  * @param attribute The attribute that shall be enforced
  */
case class AttributeEnforcer(attribute: Attribute) {
  private val enforcer = DataTypeEnforcers enforcerFor attribute.dataType match {
    case Some(value) => value
    case None =>
      throw new NoSuchElementException(s"No enforcer for ${attribute.dataType.typeName}")
  }

  /** If an option is given, returns the option, otherwise returns Option(any). */
  private def toOption(any: Any): Option[Any] = any match {
    case o: Option[_] => o
    case default => Option(default)
  }

  /** Enforces the attribute policies on the given value
    *
    * @param any The value that shall be checked and converted
    * @return The checked and converted result
    * @throws AssertionError if the value could not hold the assertions
    */
  def enforce(any: Any): Any = toOption(any) match {
    case None if !attribute.nullable =>
      throw new AssertionError("null value but attribute not nullable")

    case None => null

    case Some(value) =>
      enforcer enforce value
  }
}
