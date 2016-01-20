package org.apache.spark.sql.catalyst.plans.logical

/**
  * Parts of Distinct operations are transalted to Aggregates. Therefor this object is added
  * to make it easier to translate a given plan to an aggregate. Look into calling context
  * for more information.
  */
object IsDistinct {

  def unapply(any: Any): Option[LogicalPlan] =
    any match {
      case Distinct(child) => Some(child)
      case Aggregate(ge, ae, child) if child.output == ge && ge == ae => Some(child)
      case _ => None
    }

}
