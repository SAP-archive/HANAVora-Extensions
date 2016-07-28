package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.aggregate.TungstenAggregate
import org.apache.spark.sql.types.{DecimalType, DoubleType}

/**
  *
  * This partial aggregation matches Tungsten processed aggregates. We distinguish between final and
  * partial aggregation properties. A partial aggregation is always in two phases: the results of
  * the partial aggregation is the input to the final one.
  *
  * The returned values for this match are as follows
  * (see [[PartialAggregation.ReturnType]]):
  *  - Grouping expressions for the final aggregation (finalGroupingExpressions)
  *  - Aggregate expressions for the final aggregation (finalAggregateExpressions)
  *  - Grouping expressions for the partial aggregation. (partialGroupingExpressions)
  *  - Aggregate expressions for the partial aggregation. (partialAggregateExpressionsForPushdown)
  *  - Mapping of the aggregate functions to the
  *         appropriate attributes (aggregateFunctionToAttribute)
  *  - the rewritten result expressions of the partial aggregation
  *  - remaining logical plan (child)
  *
  * If it matches to a Partial Aggregation, the CatalystSource Strategy builds up a
  * [[TungstenAggregate]] or a [[org.apache.spark.sql.execution.aggregate.SortBasedAggregate]]
  *
  */
object PartialAggregation extends Logging {
  type ReturnType = (Seq[NamedExpression], Seq[AggregateExpression], Seq[NamedExpression],
    Seq[NamedExpression], Map[(AggregateFunction, Boolean), Attribute],
    Seq[NamedExpression], LogicalPlan)

  // scalastyle:off cyclomatic.complexity
  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {

    // Search for an aggregate function that does not support partial
    // if we find one, we are done here, we cannot go for a partial aggregation
    case logical.Aggregate(_, resultExpressions, _) if resultExpressions.flatMap(
      expr => expr.collect { case agg: AggregateExpression => agg })
      .exists(!_.aggregateFunction.supportsPartial) =>
      // no match
      logWarning("Found an aggregate function that could not be pushed down - falling back " +
        "to normal behavior")
      None

    // There could be a possible parser Bug similar to the one detected in
    // [[SparkStrategies]] - see this class for further reference
    case logical.Aggregate(_, resultExpressions, _) if resultExpressions.flatMap(
      expr => expr.collect { case agg: AggregateExpression => agg })
        .filter(_.isDistinct).map(_.aggregateFunction.children).distinct.length > 1 =>
      // This is a sanity check. We should not reach here when we have multiple distinct
      // column sets. [[org.apache.spark.sql.catalyst.analysis.DistinctAggregationRewriter]]
      // should take care of this case.
      sys.error("You hit a query analyzer bug. Please report your query to " +
        "Spark user mailing list. It is the same bug as reported in Spark Strategies, multiple " +
        "distinct column sets should be resolved and planned differently.")

    // Assuming that resultExpressions is empty, this would mean that this function basically
    // returns a plan with no result, thus we need to fix this
    // this case applies to DISTINCTS that are rewritten as an aggregate
    case logical.Aggregate(groupingExpressions, resultExpressions, child)
      if resultExpressions.isEmpty =>
      val actualResultExpressions = groupingExpressions.collect{
        case ne: NamedExpression => ne
      }
      Some(planDistributedAggregateExecution(groupingExpressions, actualResultExpressions, child))


      // This case tries to create a partial aggregation
    case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
      Some(planDistributedAggregateExecution(groupingExpressions, resultExpressions, child))

    case _ => None
  }
  // scalastyle:on cyclomatic.complexity

  // scalastyle:off cyclomatic.complexity method.length
  private def planDistributedAggregateExecution(groupingExpressions: Seq[Expression],
                                                resultExpressions: Seq[NamedExpression],
                                                child: LogicalPlan): ReturnType = {
    /**
      * A single aggregate expression might appear multiple times in resultExpressions.
      * In order to avoid evaluating an individual aggregate function multiple times, we'll
      * build a set of the distinct aggregate expressions and build a function which can
      * be used to re-write expressions so that they reference the single copy of the
      * aggregate function which actually gets computed.
      */
    val aggregateExpressions = resultExpressions.flatMap (_.collect {
        case agg: AggregateExpression => agg
      }).distinct

    // For those distinct aggregate expressions, we create a map from the
    // aggregate function to the corresponding attribute of the function.
    val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
      val aggregateFunction = agg.aggregateFunction
      val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
      (aggregateFunction, agg.isDistinct) -> attribute
    }.toMap

    val namedGroupingExpressions = groupingExpressions.map {
      case ne: NamedExpression => ne -> ne

        /** If the expression is not a NamedExpressions, we add an alias.
          * So, when we generate the result of the operator, the Aggregate Operator
          * can directly get the Seq of attributes representing the grouping expressions.
          */
      case other =>
        val existingAlias = resultExpressions.find({
          case Alias(aliasChild, aliasName) => aliasChild == other
          case _ => false
        })
        // it could be that there is already an alias, so do not "double alias"
        val mappedExpression = existingAlias match {
          case Some(alias) => alias.toAttribute
          case None => Alias(other, other.toString)()
        }
        other -> mappedExpression
    }
    val groupExpressionMap = namedGroupingExpressions.toMap

    // make expression out of the tuples
    val finalGroupingExpressions = namedGroupingExpressions.map(_._2)
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(mode = Final))

    val partialAggregateExpressionsForPushdown: Seq[NamedExpression] =
      resultExpressions.flatMap(ne => ne match {
        case a@Alias(agg: AggregateExpression, _) => rewriteAggregateExpressionsToPartial(agg, a)
        case Alias(expr: NamedExpression, _) => Seq(expr)
        case expr: NamedExpression => Seq(expr)
        case _ => Seq.empty
      })

    /** Extracted from the `Aggregation` Strategy of Spark:
      * The original `resultExpressions` are a set of expressions which may reference
      * aggregate expressions, grouping column values, and constants. When aggregate operator
      * emits output rows, we will use `resultExpressions` to generate an output projection
      * which takes the grouping columns and final aggregate result buffer as input.
      * Thus, we must re-write the result expressions so that their attributes match up with
      * the attributes of the final result projection's input row:
      */
    val rewrittenResultExpressions = resultExpressions.map { expr =>
      expr.transformDown {
        case AggregateExpression(aggregateFunction, _, isDistinct) =>
          // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
          // so replace each aggregate expression by its corresponding attribute in the set:
          aggregateFunctionToAttribute(aggregateFunction, isDistinct)
        case expression =>

          /**
            * Since we're using `namedGroupingAttributes` to extract the grouping key
            * columns, we need to replace grouping key expressions with their corresponding
            * attributes. We do not rely on the equality check at here since attributes may
            * differ cosmetically. Instead, we use semanticEquals.
             */
          groupExpressionMap.collectFirst {
            case (grpExpr, ne) if grpExpr semanticEquals expression => ne.toAttribute
          }.getOrElse(expression)
      }.asInstanceOf[NamedExpression]
    }

    // this is the [[ReturnType]]
    (finalGroupingExpressions,
      finalAggregateExpressions,
      /* *
       * final and partial grouping expressions are the same! Still needed for Tungesten/
       * SortBasedAggregate Physical plan node
       */
      finalGroupingExpressions,
      partialAggregateExpressionsForPushdown,
      aggregateFunctionToAttribute,
      rewrittenResultExpressions,
      child)
  }
  // scalastyle:on


  /**
    * This method rewrites aggregates to the corresponding partial ones. For instance an average is
    * rewritten to a sum and a count.
    *
    * @param aggregateExpression aggregate expresions to be rewritten
    * @param outerAlias alias for that expression
    * @return
    */
  private def rewriteAggregateExpressionsToPartial(aggregateExpression: AggregateExpression,
                                                   outerAlias: Alias):
  Seq[NamedExpression] = {
    val inputBuffers = aggregateExpression.aggregateFunction.inputAggBufferAttributes
    aggregateExpression.aggregateFunction match {
      case avg: Average => {
        // two: sum and count
        val sumAlias = inputBuffers(0)
        val cntAlias = inputBuffers(1)
        val typedChild = avg.child.dataType match {
          case DoubleType | DecimalType.Fixed(_, _) => avg.child
          case _ => Cast(avg.child, DoubleType)
        }
        Seq( // sum
          Alias(AggregateExpression(Sum(typedChild), mode = Partial,
            aggregateExpression.isDistinct)
            , sumAlias.name + outerAlias.name)
          (sumAlias.exprId, sumAlias.qualifiers, Some(sumAlias.metadata)),
          // count
          Alias(AggregateExpression(Count(avg.child), mode = Partial,
            aggregateExpression.isDistinct), cntAlias.name + outerAlias.name)(cntAlias.exprId,
            cntAlias.qualifiers, Some(cntAlias.metadata)))
      }
      case Count(_) | Sum(_) | Max(_) | Min(_) =>
        Seq(Alias(aggregateExpression.copy(mode = Partial),
          inputBuffers.head.name + outerAlias.name)
        (inputBuffers.head.exprId, inputBuffers.head.qualifiers, Some(inputBuffers.head.metadata)))
      case _ => throw new RuntimeException("Approached rewrite with unsupported expression")
    }

  }
}
