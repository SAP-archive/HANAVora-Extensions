package org.apache.spark.sql.catalyst.plans.logical

object CompatDistinct {

  def unapply(any: Any): Option[LogicalPlan] =
    any match {
      case Distinct(child) => Some(child)
      case Aggregate(ge, ae, child) if child.output == ge && ge == ae => Some(child)
      case _ => None
    }

}
