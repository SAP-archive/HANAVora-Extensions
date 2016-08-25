package org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PlanUtils._
import org.apache.spark.sql.util.CollectionUtils.RichIterable

import scala.collection.immutable.Queue

/**
  * Resolves [[AnnotatedAttribute]] in the query by applying a set of rules.
  *
  * 1. Iterate over the logical plan and transfer the metadata from any
  *     [[AnnotatedAttribute]] to its child.
  *
  * 2. Since the [[AnnotatedAttribute]]s block spark from resolving some plans,
  *    we have to do some custom resolution to not have anything unresolved.
  *
  * 3. Reconstruct the relations between aliases and attribute references. To do
  *    we traverse the logical plan bottom up and extract the metadata associated
  *    with its tree-sequence into a map.
  *
  * 4. Apply the collected metadata on the logical plan top-down.
  *
  * TODO (YH, AC) we need to add an extra resolution rule for dimension metadata.
  * The idea is to add a flag to the annotated attribute that marks it as
  * generating-dimension-keyword annotation. This flag shall be evaluated here.
  */
case class ResolveAnnotations(analyzer: Analyzer) extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (hasAnnotatedAttributes(plan)) {
      /**
        * We have to do some additional resolution logic since the
        * annotated attributes block spark from analyzing some logical plans.
        */
      val withAdditionalResolution = resolve(plan)
      val withAppliedAnnotatedAttributes = applyAnnotatedAttributes(withAdditionalResolution)
      assert(!hasAnnotatedAttributes(withAppliedAnnotatedAttributes))
      val pruned = prune(withAppliedAnnotatedAttributes)
      val metadata = aggregateMetadata(pruned)
      setMetadata(withAppliedAnnotatedAttributes, metadata)
    } else {
      plan
    }
  }

  private[sql] def resolve(plan: LogicalPlan): LogicalPlan = {
    val postResolver = new RuleExecutor[LogicalPlan] {
      val fixedPoint = new FixedPoint(analyzer.fixedPoint.maxIterations)

      override protected val batches: Seq[Batch] =
        Seq(new Batch(
          "post_resolution",
          fixedPoint,
          analyzer.ResolveAliases,
          analyzer.ResolveRelations,
          analyzer.ResolveReferences))
    }

    postResolver.execute(plan)
  }

  private[sql] def hasAnnotatedAttributes(plan: LogicalPlan): Boolean = {
    plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[AnnotatedAttribute])))
  }

  private[sql] def prune(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case Union(left, _) => left
    case default => default
  }

  private[sql] def applyAnnotatedAttributes(plan: LogicalPlan): LogicalPlan = {
    plan transformUp {
      case node =>
        node transformExpressionsUp {
          case attribute@AnnotatedAttribute(expr) => expr match {
            case reference: AttributeReference =>
              sys.error(s"I have annotated attribute $attribute that references an " +
                s"attribute reference $reference")
            case alias: Alias =>
              withMetadata(alias, attribute.metadata)
          }
        }
    }
  }

  private[sql] def collectNamedExpressions(plan: LogicalPlan): Queue[NamedExpression] = {
    plan.toPostOrderSeq.foldLeft(Queue.empty[NamedExpression]) {
      case (acc, node) =>
        acc ++ node.expressions.flatMap(_.toPostOrderSeq).collect {
          case n: NamedExpression => n
        }
    }.distinct
  }

  private[sql] def aggregateMetadata(plan: LogicalPlan) = {
    collectNamedExpressions(plan).foldLeft(Map.empty[Seq[ExprId], Metadata]) {
      case (acc, item) =>
        exprIdOf(item).flatMap { targetId =>
          acc.find { case (seq, _) => seq.contains(targetId) }
        } match {
          // Only aliases may override
          case Some((k, v)) if item.isInstanceOf[Alias] =>
            val newSeq = k.lastOption
              .filter(_ != item.exprId)
              .map(_ => k :+ item.exprId)
              .getOrElse(k)
            acc + (newSeq -> MetadataAccessor.propagateMetadata(v, item.metadata))
          case default if item.resolved && !acc.keys.exists(_.contains(item.exprId)) =>
            acc + (Seq(item.exprId) -> item.metadata)
          case _ =>
            acc
        }
    }
  }

  private[sql] def exprIdOf(expr: NamedExpression): Option[ExprId] = expr match {
    case Alias(child: NamedExpression, _) if child.resolved => Some(child.exprId)
    case default if default.resolved => Some(default.exprId)
    case _ => None
  }

  // scalastyle:off cyclomatic.complexity
  private[sql] def setMetadata(plan: LogicalPlan, sets: Map[Seq[ExprId], Metadata]): LogicalPlan = {
    plan transformUp {
      case lp =>
        lp transformExpressionsDown {
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
    }
  }
  // scalastyle:on cyclomatic.complexity

  /**
   * Sets the metadata of the given [[AttributeReference]].
   *
   * @param attr An [[AttributeReference]].
   * @param newMetadata New metadata.
   * @return New [[AttributeReference]] with new metadata.
   */
  private[this] def withMetadata(
                                    attr: AttributeReference,
                                    newMetadata: Metadata): AttributeReference =
    attr.copy(dataType = attr.dataType, nullable = attr.nullable, metadata =
      newMetadata)(exprId = attr.exprId, qualifiers = attr.qualifiers
    )

  /**
    * Sets the metadata of the given [[Alias]].
    *
    * @param attr An [[Alias]].
    * @param newMetadata New metadata.
    * @return New [[Alias]] with new metadata.
    */
  private[this] def withMetadata(attr: Alias,
                                 newMetadata: Metadata): Alias =
    attr.copy(attr.child)(attr.exprId, attr.qualifiers, Some(newMetadata))
}
