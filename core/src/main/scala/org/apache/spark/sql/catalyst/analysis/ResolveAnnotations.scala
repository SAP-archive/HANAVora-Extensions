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
  * 1. Iterate over the logical plan and if we find any [[AnnotatedAttribute]] node
  *    that has [[AttributeReference]] child then we will transfer its meta
  *    data to the [[AttributeReference]] child.
  *
  * 2. Once we have removed all [[AnnotatedAttribute]]s we will fix the metadata
  *    of the [[AttributeReference]]s by iterating the tree again and transferring
  *    the metadata bottom-up.
  *     When this step is done we will apply proper propagation of metadata to all
  *     related [[Attribute]]s. Finally the top of the tree will have the final
  *     transformed metadata.
  *
  * 3. Remove all [[AnnotatedAttribute]]s from the logical plan.
  *
  * 4. Copy the annotations again to circumvent optimizer changes to the tree.
  *    The annotations are broadcasted from any top-level [[AttributeReference]] to
  *    lower-level [[AttributeReference]] if they are related indirectly to each
  *    other by a alias.
  *
  *
  * Some extra logic is done here:
  *    - When transferring meta data from one node to the other, we have to take
  *      various special cases into account:
  *      1. if both metadata have the same key/value pair, then the newer one
  *         overrides it.
  *      2. if the newer metadata has a key=* and some value, then we override
  *         all older metadata with that value before transferring it.
  *    - Filter nodes are kept, it seems they are not causing the optimizer to
  *      freak out.
  *
  * TODO (YH) AttributeReferences must be resolved correctly. We will need a
  * separate resolver for them.
  */
object ResolveAnnotations extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if(plan.resolved) {
      val result1 = transferMetadataToBottom(plan)
      val result2 = fixAttributeReferencesMetadata(result1)
      val result3 = collapseAnnotatedAttributes(result2)
      val result4 = broadcastMetadataDownTheTree(result3)
      result4
    } else {
      plan
    }
  }

  /**
    * Moves metadata from [[AnnotatedAttribute]] to the underlying [[AttributeReference]].
   */
  private[sql] def transferMetadataToBottom(plan: LogicalPlan): LogicalPlan = {
    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan =>
        val exprIdMetadataMap = getAnnotations(lp)
        // move metadata from AnnotatedAttribute to the AttributeReference.
        val planFixed = lp transformExpressionsUp {
          case attr: AttributeReference =>
            exprIdMetadataMap.get(attr.exprId) match {
              case Some(q) =>
                logInfo(s"Using new meta ($q) for attribute: $attr")
                val b = withMetadata(attr, q)
                logInfo(s"changed $attr to $b")
                b
              case None =>
                attr
            }
        }
        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan2(removeExpressionPrefixes(planFixed))
    }
    val temp = removeDummyPlans2(transformedPlan)
    temp
  }

  /**
    * In this method we re-pick all Aliases under [[AnnotatedAttribute]] and create a map of their
    * exprId and the metadata of the parent annotated attribute. Then any other
    * [[AttributeReference]] that is using them will have that metadata instead.
    *
    * This is implemented to fix the issue that happens when an [[AttributeReference]] is
    * referencing an alias inside a subquery (e.g. in a JOIN query). The newly created
    * attribute reference will pick up the older alias metadata because it is created before
    * our rule. Therefor after we push the annotations from the [[AnnotatedAttribute]] to their
    * respective [[AttributeReference]] the alias will have a new metadata that does not
    * necessarily correspond to the one in the referencing [[AttributeReference]]. That is
    * why we have to fix it using this tree visit.
    *
    * @param plan the logical plan.
    */
  private[sql] def fixAttributeReferencesMetadata(plan: LogicalPlan): LogicalPlan = {
    val expressionsMap = collectMetadata(plan)
    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan =>
        val planFixed = lp transformExpressionsDown {
          case attr:AttributeReference =>
            val newAttr = expressionsMap.get(attr.exprId) match {
              case Some(m) if MetadataAccessor.nonEmpty(m) => withMetadata(attr, m)
              case _ => attr
            }
            newAttr
          case p => p
        }
        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan2(removeExpressionPrefixes(planFixed))
    }
    val temp = removeDummyPlans2(transformedPlan)
    temp
  }

  /**
    * The analysis can collapse nested projections if they are the same. Which is problematic
    * because the most recent metadata is propagated up the tree and due to this rule we will
    * lose that metadata. Therefor we apply this tree visitation that will copy the metadata
    * from upper [[AttributeReference]] to lower [[AttributeReference]]s that share the same
    * references.
    */
  private[sql] def broadcastMetadataDownTheTree(plan: LogicalPlan): LogicalPlan = {
    val expressionsMap = finalMetadataMap(plan)
    val transformedPlan = plan transformUp {
      case lr@IsLogicalRelation(_) => lr
      case lp: LogicalPlan =>
        val planFixed = lp transformExpressionsDown {
          case attr:AttributeReference =>
            val newAttr = expressionsMap.get(attr.exprId) match {
              case Some(m) if MetadataAccessor.nonEmpty(m) => withMetadata(attr, m)
              case _ => attr
            }
            newAttr
          case p => p
        }
        /** Now we need to delete the prefix in all the attributes. SPARK-8658. See below. */
        DummyPlan2(removeExpressionPrefixes(planFixed))
    }
    val temp = removeDummyPlans2(transformedPlan)
    temp
  }

  /**
   * Removes all [[AnnotatedAttribute]] nodes.
   * @param plan The logical plan.
   * @return The 'cleansed' logical plan without [[AnnotatedAttribute]].
   */
  private[sql] def collapseAnnotatedAttributes(plan: LogicalPlan): LogicalPlan = {
    val result = plan transformUp {
      case Project(projectList, child) =>
        val resolvedProjectionList = projectList.map {
          /* if !hasUnresolvedAnnotationReferences(i) */
          case AnnotatedAttribute(x:NamedExpression) => x
          case x:Expression => x
        }
        Project(resolvedProjectionList, child)
      case p => p
    }
    result
  }

  /**
    * Returns a [[Map]] containing the [[ExprId]] of all [[AnnotatedAttribute]]s in the plan
    * with the consolidated metadata with the underlying [[AttributeReference]].
    *
    * @param plan The plan.
    * @return
    */
  private def getAnnotations(plan: LogicalPlan): Map[ExprId, Metadata] = {
    val result = new mutable.HashMap[ExprId, Metadata].empty
    plan.foreachUp {
      case Project(list, _) => list.collect {
        case item:AnnotatedAttribute =>
          // Maybe I can just pattern-match the item like this:
          // item@AnnotatedExpression(a@Alias(b@AttributeReference(...))) ...
          val attr = getReferencedAttribute(item.child)
          result.update(attr.exprId, MetadataAccessor.propagateMetadata(attr.metadata,
            MetadataAccessor.expressionMapToMetadata(item.annotations)))
      }
      case _ =>
    }
    result.toMap
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

  /**
    * Iterates the tree (bottom-up) and for each [[AnnotatedAttribute]] with an [[Alias]].
    *
    * It works as follows: When an [[AnnotatedAttribute]] with an [[Alias]] is encountered it first
    * checks whether the alias's id already exists in the map. If so then it propagates the (old)
    * alias metadata to the (new, since we visit the tree bottom-up) annotated attribute metadata
    * and registers the resulting metadata with the alias's id in the map.
    *
    * @param plan The logical plan.
    * @return a map from aliases' ids to their metadata.
    */
  private def collectMetadata(plan: LogicalPlan): Map[ExprId, Metadata] = {
    val result = new mutable.HashMap[ExprId, Metadata].empty
    plan.foreachUp {
      case Project(list, _) => list.collect {
        case item@AnnotatedAttribute(a:Alias) =>
          val oldMetadata = result.getOrElse(item.exprId, new MetadataBuilder().build())
          result.update(a.exprId, MetadataAccessor.propagateMetadata(oldMetadata, item.metadata))
      }
      case _ =>
    }
    result.toMap
  }

  // scalastyle:off cyclomatic.complexity
  private def finalMetadataMap(plan: LogicalPlan): Map[ExprId, Metadata] = {
    val result = new mutable.HashMap[ExprId, Metadata].empty
    plan.foreach {
      case Project(list, _) => list.collect {
        case a:NamedExpression if a.resolved =>
          a.references.collect {
            case attr:Attribute if !result.contains(a.exprId) =>
              result.put(attr.exprId, a.metadata)
          }
          a
      }
      case _ =>
    }
    // the map might contains references to aliases. In this case we have to
    // expand these entries with the corresponding references of these aliases.
    plan.foreach {
      case Project(list, _) => list.collect {
        case a:Alias if result.contains(a.exprId) =>
          a.references.collect {
            case attr:Attribute if !result.contains(attr.exprId) =>
            result.put(attr.exprId, result.get(a.exprId).get)
          }
          a
      }
      case _ =>
    }
    result.toMap
  }

  private def getReferencedAttribute(exp: Expression): NamedExpression = {
    exp match {
      case a:Alias => getReferencedAttribute(a.child)
      case a:AttributeReference => a
      case _ => sys.error(s"'${exp.simpleString}' expression is not valid for annotations.")
    }
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

  /** XXX: Remove prefix from all expression names. SPARK-8658. */
  private[this] def removeExpressionPrefixes(plan: LogicalPlan): LogicalPlan =
    plan transformExpressionsDown {
      case attr: AttributeReference =>
        attr.copy(name = attr.name.replaceFirst(PREFIX, ""))(
          exprId = attr.exprId, qualifiers = attr.qualifiers
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
