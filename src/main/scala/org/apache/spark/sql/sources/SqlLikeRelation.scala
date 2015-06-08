package org.apache.spark.sql.sources

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, NamedExpression}

/**
 * Marks a relation as SQL like and adds common functions
 */
trait SqlLikeRelation {

  def tableName: String

  /**
   * Preparing aggregation and grouping expressions to work on data source and
   * setting missing aliases when needed
   * @param aggregationExpressions
   * @param groupingExpressions
   * @return Tuple with modified aggregation and grouping expressions
   */
  protected def prepareAggregationsUsingAliases(aggregationExpressions: Seq[NamedExpression],
                                                groupingExpressions: Seq[Expression]):
  (Seq[NamedExpression], Seq[Expression]) = {
    /*
     * For references to attributes we have to omit the expression id to reference the correct
     * column in velocity, thus we remove it here (set it to null
     */
    (
      aggregationExpressions.map({
        case ne: AttributeReference => AttributeReference(ne.name, ne.dataType)(exprId = null)
        /* if it is an alias it will stay that way */
        case al: Alias => al.copy(al.child, s"${al.name}EID${al.exprId.id}")(al.exprId,
          al.qualifiers, al.explicitMetadata)
        /* if it is not an alias we rewrite it
         *
         * It will get a new expression id by Spark SQL (is unique!) AND we add the expression id of
         * the named expression (which is unique, too) */
        case ne => new Alias(ne, s"${ne.name}EID${ne.exprId.id}")()
      })
      ,

      /*
       * fix group by expressions if they contain any udf
       *
       * if a UDF is in the group by expressions we replace replace it by the accompanying alias
       * in the pc
       *
       */
      groupingExpressions.map({
        case e: AttributeReference => AttributeReference(e.name, e.dataType)(exprId = null)
        case e =>
          val alias = aggregationExpressions.find(ne => ne.isInstanceOf[Alias] &&
            ne.asInstanceOf[Alias].child.equals(e)).getOrElse(
              throw new RuntimeException("Cannot resolve Alias for " + e + "! UDFs in " +
                "GROUP BY clauses are not supported by Velocity")).asInstanceOf[Alias]
          AttributeReference(alias.name + "EID" + alias.exprId.id, e.dataType)(exprId =
            alias.exprId)
      })
      )
  }
}
