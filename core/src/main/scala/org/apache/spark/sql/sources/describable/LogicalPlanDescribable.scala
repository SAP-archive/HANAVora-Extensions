package org.apache.spark.sql.sources.describable

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._

/**
  * Default implementations to describe [[LogicalPlan]]s.
  */
trait LogicalPlanDescribable extends Describable {
  val plan: LogicalPlan

  private lazy val structureDescriber =
    if (!plan.resolved || plan.output.isEmpty) None
    else Some(StructureDescriber(plan.output))

  override final def describe(): Any = {
    val defaultValues =
      plan.nodeName +:
      structureDescriber.map(_.describe()).toSeq

    val withChildValues =
      if (children.isEmpty) defaultValues
      else defaultValues :+ Row.fromSeq(children.map(child => child.describe()))

    Row.fromSeq(withChildValues ++ additionalValues)
  }

  def additionalValues: Seq[Any]

  override final def describeOutput: DataType = {
    StructType(
      LogicalPlanDescribable.defaultFields ++
        structureDescriber.map(d => StructField("fields", d.describeOutput)).toSeq ++
        childField.toSeq ++
        additionalFields)
  }

  def additionalFields: Seq[StructField]

  lazy final val children: Seq[Describable] = plan.children.map(Describable.apply)

  lazy final val childField: Option[StructField] = {
    if (children.nonEmpty) {
      Some(
        StructField("children",
          StructType(children.zipWithIndex.map {
          case (describable, index) =>
            StructField(s"child_$index", describable.describeOutput)
        })))
    } else {
      None
    }
  }
}

object LogicalPlanDescribable {
  val defaultFields: Seq[StructField] =
    StructField("name", StringType, nullable = false) :: Nil
}

trait NoAdditions {
  self: LogicalPlanDescribable =>
  override def additionalFields: Seq[StructField] = Nil

  override def additionalValues: Seq[Any] = Nil
}

