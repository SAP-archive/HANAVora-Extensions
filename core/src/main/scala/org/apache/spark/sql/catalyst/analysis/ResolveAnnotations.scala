package org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PlanUtils._

import scala.collection.immutable.Queue
import org.apache.spark.sql.util.CollectionUtil.RichSeq

/**
  * Resolves [[AnnotatedAttribute]] in the query by applying a set of rules.
  *
  * 1. Iterate over the logical plan and transfer the metadata from any
  *     [[AnnotatedAttribute]] to its child.
  *
  * 2. Reconstruct the relations between aliases and attribute references. To do
  *    we traverse the logical plan bottom up and extract the metadata associated
  *    with its tree-sequence into a map.
  *
  * 3. Apply the collected metadata on the logical plan top-down.
  *
  * TODO (YH, AC) we need to add an extra resolution rule for dimension metadata.
  * The idea is to add a flag to the annotated attribute that marks it as
  * generating-dimension-keyword annotation. This flag shall be evaluated here.
  */
object ResolveAnnotations extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (shouldApply(plan)) {
      val withAppliedAnnotatedAttributes = applyAnnotatedAttributes(plan)
      val pruned = prune(withAppliedAnnotatedAttributes)
      val metadata = aggregateMetadata(pruned)
      setMetadata(withAppliedAnnotatedAttributes, metadata)
    } else {
      plan
    }
  }

  private[sql] def shouldApply(plan: LogicalPlan): Boolean = {
    plan.resolved && hasAnnotatedAttributes(plan)
  }

  private[sql] def hasAnnotatedAttributes(plan: LogicalPlan): Boolean = {
    plan.find {
      case Project(pl, _) => pl.exists(_.isInstanceOf[AnnotatedAttribute])
      case _ => false
    }.isDefined
  }

  private[sql] def prune(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Union(left, _) => left
    case default => default
  }

  private[sql] def applyAnnotatedAttributes(plan: LogicalPlan): LogicalPlan = {
    removeDummyPlans2(plan transformUp {
      case node =>
        val planFixed = node transformExpressionsUp {
          case attribute@AnnotatedAttribute(expr) => expr match {
            case reference: AttributeReference =>
              sys.error(s"I have annotated attribute $attribute that references an " +
                s"attribute reference $reference")
            case alias: Alias =>
              withMetadata(alias, attribute.metadata)
          }
        }
        DummyPlan2(removeExpressionPrefixes(planFixed))
    })
  }

  private[sql] def collectNamedExpressions(plan: LogicalPlan): Queue[NamedExpression] = {
    plan.toPostOrderSeq.foldLeft(Queue.empty[NamedExpression]) {
      case (acc, node) =>
        acc ++ node.expressions.flatMap(_.toPostOrderSeq).collect {
          case n: NamedExpression => n
        }
    }.orderPreservingDistinct
  }

  private[sql] def aggregateMetadata(plan: LogicalPlan) = {
    collectNamedExpressions(plan).foldLeft(Map.empty[Seq[ExprId], Metadata]) {
      case (acc, item) =>
        val targetId = item match {
          case Alias(child: NamedExpression, _) => child.exprId
          case default => default.exprId
        }
        acc.find {
          case (seq, _) => seq.contains(targetId)
        } match {
          // Only aliases may override
          case Some((k, v)) if item.isInstanceOf[Alias] =>
            val newSeq = k.lastOption
              .filter(_ != item.exprId)
              .map(_ => k :+ item.exprId)
              .getOrElse(k)
            acc + (newSeq -> MetadataAccessor.propagateMetadata(v, item.metadata))
          case default if !acc.keys.exists(_.contains(item.exprId)) =>
            acc + (Seq(item.exprId) -> item.metadata)
          case _ =>
            acc
        }
    }
  }

  // scalastyle:off cyclomatic.complexity
  private[sql] def setMetadata(plan: LogicalPlan, sets: Map[Seq[ExprId], Metadata]): LogicalPlan = {
    val transformedPlan = plan transformUp {
      case lp =>
        val planFixed = lp transformExpressionsDown {
          case attr:AttributeReference =>
            sets.find {
              case (k, v) => k.contains(attr.exprId)
            } match {
              case Some((_, metadata)) => withMetadata(attr, metadata)
              case None => attr
            }
          case attr:Alias =>
            sets.find {
              case (k, v) => k.exists(e => e.equals(attr.exprId))
            } match {
              case Some((_, metadata)) => withMetadata(attr, metadata)
              case None => attr
            }
          case p => p
        }
        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan2(removeExpressionPrefixes(planFixed))
    }
    val temp = removeDummyPlans2(transformedPlan)
    temp
  }
  // scalastyle:on cyclomatic.complexity

  //
  // Code to workaround SPARK-8658.
  // https://issues.apache.org/jira/browse/SPARK-8658
  // We need these tricks so that transformExpressionsDown
  // works when only qualifiers changed. Currently, Spark ignores
  // changes affecting only qualifiers.
  //

  /** XXX: Prefix to append temporarily (SPARK-8658) */
  private val PREFIX = "XXX___"

  /**
   * Adds a qualifier to an attribute.
   * Workarounds SPARK-8658.
   *
   * @param attr An [[AttributeReference]].
   * @param newMetadata New qualifiers.
   * @return New [[AttributeReference]] with new qualifiers.
   */
  private[this] def withMetadata(
                                    attr: AttributeReference,
                                    newMetadata: Metadata): AttributeReference =
    attr.copy(name = PREFIX.concat(attr.name), attr.dataType, attr.nullable, metadata =
      newMetadata)(exprId = attr.exprId, qualifiers = attr.qualifiers
    )

  private[this] def withMetadata(attr: Alias,
                                 newMetadata: Metadata): Alias =
    attr.copy(attr.child, PREFIX.concat(attr.name))(attr.exprId, attr.qualifiers, Some(newMetadata))

  /** XXX: Remove prefix from all expression names. SPARK-8658. */
  private[this] def removeExpressionPrefixes(plan: LogicalPlan): LogicalPlan =
    plan transformExpressionsDown {
      case attr: AttributeReference =>
        attr.copy(name = attr.name.replaceFirst(PREFIX, ""))(
          exprId = attr.exprId, qualifiers = attr.qualifiers
        )
      case attr: Alias =>
        attr.copy(child = attr.child, name = attr.name.replaceFirst(PREFIX, ""))(
          exprId = attr.exprId, qualifiers = attr.qualifiers,
          explicitMetadata = attr.explicitMetadata
        )
    }

  /** XXX: Used to force change on transformUp (SPARK-8658) */
  private case class DummyPlan2(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
  }

  /** XXX: Remove all [[DummyPlan2]] (SPARK-8658) */
  private def removeDummyPlans2(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      /* TODO: This is duplicated here */
      case Subquery(name, Subquery(innerName, child)) =>
        /* If multiple subqueries, preserve the outer one */
        Subquery(name, child)
      case DummyPlan2(child) => child
      case p => p
    }
}
