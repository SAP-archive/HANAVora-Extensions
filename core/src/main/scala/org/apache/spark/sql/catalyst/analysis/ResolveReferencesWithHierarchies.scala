package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * This is a copy of Spark's [[Analyzer#ResolveReferences]] adding
  * [[Hierarchy]] handling.
  */
private[sql]
case class ResolveReferencesWithHierarchies(analyzer: Analyzer) extends Rule[LogicalPlan] {

  // scalastyle:off cyclomatic.complexity
  // scalastyle:off method.length
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p: LogicalPlan if !p.childrenResolved => p

    // If the projection list contains Stars, expand it.
    case p@Project(projectList, child) if containsStar(projectList) =>
      Project(
        projectList.flatMap {
          case s: Star => s.expand(child.output, analyzer.resolver)
          case Alias(f@UnresolvedFunction(_, args), name) if containsStar(args) =>
            val expandedArgs = args.flatMap {
              case s: Star => s.expand(child.output, analyzer.resolver)
              case o => o :: Nil
            }
            Alias(child = f.copy(children = expandedArgs), name)() :: Nil
          case Alias(c@CreateArray(args), name) if containsStar(args) =>
            val expandedArgs = args.flatMap {
              case s: Star => s.expand(child.output, analyzer.resolver)
              case o => o :: Nil
            }
            Alias(c.copy(children = expandedArgs), name)() :: Nil
          case Alias(c@CreateStruct(args), name) if containsStar(args) =>
            val expandedArgs = args.flatMap {
              case s: Star => s.expand(child.output, analyzer.resolver)
              case o => o :: Nil
            }
            Alias(c.copy(children = expandedArgs), name)() :: Nil
          case o => o :: Nil
        },
        child)
    case t: ScriptTransformation if containsStar(t.input) =>
      t.copy(
        input = t.input.flatMap {
          case s: Star => s.expand(t.child.output, analyzer.resolver)
          case o => o :: Nil
        }
      )

    // If the aggregate function argument contains Stars, expand it.
    case a: Aggregate if containsStar(a.aggregateExpressions) =>
      a.copy(
        aggregateExpressions = a.aggregateExpressions.flatMap {
          case s: Star => s.expand(a.child.output, analyzer.resolver)
          case o => o :: Nil
        }
      )

    // Special handling for cases when self-join introduce duplicate expression ids.
    case j@Join(left, right, _, _) if left.outputSet.intersect(right.outputSet).nonEmpty =>
      val conflictingAttributes = left.outputSet.intersect(right.outputSet)
      logDebug(s"Conflicting attributes ${conflictingAttributes.mkString(",")} in $j")

      val (oldRelation, newRelation) = right.collect {
        // Handle base relations that might appear more than once.
        case oldVersion: MultiInstanceRelation
          if oldVersion.outputSet.intersect(conflictingAttributes).nonEmpty =>
          val newVersion = oldVersion.newInstance()
          (oldVersion, newVersion)

        // Handle projects that create conflicting aliases.
        case oldVersion@Project(projectList, _)
          if findAliases(projectList).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(projectList = newAliases(projectList)))

        case oldVersion@Aggregate(_, aggregateExpressions, _)
          if findAliases(aggregateExpressions).intersect(conflictingAttributes).nonEmpty =>
          (oldVersion, oldVersion.copy(aggregateExpressions = newAliases(aggregateExpressions)))

        case oldVersion: Generate
          if oldVersion.generatedSet.intersect(conflictingAttributes).nonEmpty =>
          val newOutput = oldVersion.generatorOutput.map(_.newInstance())
          (oldVersion, oldVersion.copy(generatorOutput = newOutput))

        case oldVersion@Window(_, windowExpressions, _, child)
          if AttributeSet(windowExpressions.map(_.toAttribute)).intersect(conflictingAttributes)
            .nonEmpty =>
          (oldVersion, oldVersion.copy(windowExpressions = newAliases(windowExpressions)))

        /** HERE IS OUR ADDED CASE */
        case oldVersion@Hierarchy(relation, childAlias, parenthoodExpression,
        searchBy, startWhere, nodeAttr) if conflictingAttributes.contains(nodeAttr) =>
          (oldVersion, oldVersion.copy(nodeAttribute = nodeAttr.newInstance()))

      }.headOption.getOrElse {
        // Only handle first case, others will be fixed on the next pass.
        sys.error(
          s"""
             |Failure when resolving conflicting references in Join:
             |$plan
              |
              |Conflicting attributes: ${conflictingAttributes.mkString(",")}
              """.stripMargin)
      }

      val attributeRewrites = AttributeMap(oldRelation.output.zip(newRelation.output))
      val newRight = right transformUp {
        case r if r == oldRelation => newRelation
      } transformUp {
        case other => other transformExpressions {
          case a: Attribute => attributeRewrites.get(a).getOrElse(a)
        }
      }
      j.copy(right = newRight)

    case q: LogicalPlan =>
      logTrace(s"Attempting to resolve ${q.simpleString}")
      q transformExpressionsUp {
        case u@UnresolvedAttribute(nameParts) if nameParts.length == 1 &&
          analyzer.resolver(nameParts(0), VirtualColumn.groupingIdName) &&
          q.isInstanceOf[GroupingAnalytics] =>
          // Resolve the virtual column GROUPING__ID for the operator GroupingAnalytics
          q.asInstanceOf[GroupingAnalytics].gid
        case u@UnresolvedAttribute(nameParts) =>
          // Leave unchanged if resolution fails.  Hopefully will be resolved next round.
          val result =
            withPosition(u) {
              q.resolveChildren(nameParts, analyzer.resolver).getOrElse(u)
            }
          logDebug(s"Resolving $u to $result")
          result
        case UnresolvedExtractValue(child, fieldExpr) if child.resolved =>
          ExtractValue(child, fieldExpr, analyzer.resolver)
      }
  }
  // scalastyle:on cyclomatic.complexity
  // scalastyle:on method.length

  def newAliases(expressions: Seq[NamedExpression]): Seq[NamedExpression] = {
    expressions.map {
      case a: Alias => Alias(a.child, a.name)()
      case other => other
    }
  }

  def findAliases(projectList: Seq[NamedExpression]): AttributeSet = {
    AttributeSet(projectList.collect { case a: Alias => a.toAttribute })
  }

  /**
   * Returns true if `exprs` contains a [[Star]].
   */
  protected def containsStar(exprs: Seq[Expression]): Boolean =
    exprs.exists(_.collect { case _: Star => true }.nonEmpty)
}
