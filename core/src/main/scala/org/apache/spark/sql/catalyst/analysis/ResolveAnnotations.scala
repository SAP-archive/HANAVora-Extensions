package org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.IsLogicalRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String


import scala.collection.mutable

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
  */
object ResolveAnnotations extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (shouldApply(plan)) {
      val (result1, updatedAliases) = transferMetadataToImmediateChild(plan)
      val map = buildMetadataSets(result1, updatedAliases)
      val result2 = setMetadata(result1, map)
      result2
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

  // scalastyle:off cyclomatic.complexity
  private[sql] def buildMetadataSets(plan: LogicalPlan, updatedAliases: Set[ExprId])
    : Map[Seq[ExprId], Metadata] = {

    var resultingMap = new mutable.HashMap[Seq[ExprId], Metadata]()
    plan foreachUp {
      case lr@IsLogicalRelation(_) =>
        lr.output.foreach(attr => resultingMap += (Seq(attr.exprId) -> attr.metadata))
      case Project(projectList, child) =>
        projectList.foreach {

          case al@Alias(a:AttributeReference, _) =>
            resultingMap.find {
              case (seq, _) => seq.nonEmpty && seq.last.equals(a.exprId)
            } match {
              case Some((k, v)) if updatedAliases.contains(al.exprId)  =>
                resultingMap +=
                  ((k :+ al.exprId) -> MetadataAccessor.propagateMetadata(v, al.metadata))
              case Some((k, v)) =>
                resultingMap +=
                  ((k :+ al.exprId) -> v)
              case None =>
                sys.error(s"the attribute $al does not have any reference in $resultingMap")
            }

          case ar:AttributeReference =>
            resultingMap.find {
              case (seq, _) => seq.nonEmpty && seq.last.equals(ar.exprId)
            } match {
              case None =>
                resultingMap += (Seq(ar.exprId) -> ar.metadata)
              case default => default
            }

          case default => default
        }
      case default =>
    }

    // remove redundant sets.
    resultingMap.keys.collect {
      case k if resultingMap.keySet.exists(s => s.size > k.size && s.startsWith(k)) =>
        resultingMap -= k
    }

    resultingMap.toMap
  }

  private[sql] def setMetadata(plan: LogicalPlan, sets: Map[Seq[ExprId], Metadata]): LogicalPlan = {
    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan =>
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

  private[sql] def transferMetadataToImmediateChild(plan: LogicalPlan)
    : (LogicalPlan, Set[ExprId]) = {
    var seq = new mutable.HashSet[ExprId]()

    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan =>
        // move metadata from AnnotatedAttribute to the AttributeReference.
        val planFixed = lp transformExpressionsUp {
          case an@AnnotatedAttribute(al:Alias) =>
            seq += al.exprId
            withMetadata(al, an.metadata)
          case an@AnnotatedAttribute(ar:AttributeReference) =>
            sys.error(s"I have annotated attribute $an that references an attribute reference $ar")
          case default => default
        }
        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan2(removeExpressionPrefixes(planFixed))
    }
    val temp = removeDummyPlans2(transformedPlan)
    (temp, seq.toSet)
  }

  // not used for now (should be removed?) (YH)
  def hasUnresolvedAnnotationReferences(attr: AnnotatedAttribute): Boolean = {
    attr.annotations.exists(p => p._2.isInstanceOf[AnnotationReference] && !p._2.resolved)
  }

  /**
   * Transforms a metadata map to [[Metadata]] object which is used in table attributes.
    *
    * @param metadata the metadata map.
   * @return the [[Metadata]] object.
   */
  protected def toTableMetadata(metadata: Map[String, Expression]): Metadata = {
    val res = new MetadataBuilder()
    metadata.foreach {
      case (k, v:Literal) =>
        v.dataType match {
          case StringType => res.putString(k, v.value.asInstanceOf[UTF8String].toString())
          case IntegerType => res.putLong(k, v.value.asInstanceOf[Long])
          case DoubleType => res.putDouble(k, v.value.asInstanceOf[Double])
          case NullType => res.putString(k, null)
        }
      case (k, v:AnnotationReference) =>
        sys.error("column metadata can not have a reference to another column metadata")
    }
    res.build()
  }

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

  private[this] def withMetadata(
                                  attr: Alias,
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
