package org.apache.spark.sql.parser

import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

private[sql] class SapParserDialect extends ParserDialect {

  override def parse(sqlText: String): LogicalPlan = SapDQLParser.parse(sqlText)

}
