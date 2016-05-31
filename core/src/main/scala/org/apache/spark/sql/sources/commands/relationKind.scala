package org.apache.spark.sql.sources.commands

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

sealed trait RelationKind extends Product {
  def name: String = productPrefix
}

case object Table extends RelationKind

case object View extends RelationKind

object RelationKind {
  def unapply(arg: String): Option[RelationKind] = arg match {
    case "table" => Some(Table)
    case "view" => Some(View)
    case _ => None
  }

  def typeOf(plan: LogicalPlan): String = plan.collectFirst {
    case w: WithExplicitRelationKind => w.relationKind.name
  }.getOrElse(Table.name)
}

sealed trait WithExplicitRelationKind {
  def relationKind: RelationKind
}

object WithExplicitRelationKind {
  def unapply(arg: LogicalPlan): Option[RelationKind] = arg.collectFirst {
    case w: WithExplicitRelationKind => w.relationKind
    case LogicalRelation(base: WithExplicitRelationKind, _) => base.relationKind
  }
}

trait Table extends WithExplicitRelationKind {
  override def relationKind: RelationKind = Table
}

trait View extends WithExplicitRelationKind {
  override def relationKind: RelationKind = View
}
