package org.apache.spark.sql.catalyst.expressions.tablefunctions

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project, Subquery}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.NullType

/** Physical plan of describing a table
  *
  * @param plan The logical plan to describe.
  */
case class RunDescribeTable(plan: LogicalPlan)
  extends PhysicalTableFunction {
  override protected def run(): Seq[Row] = {
    val tableCatalog = plan.tableCatalog
    val tableSchema = plan.tableSchema
    val tableName = plan.tableName
    val columns = plan.columns

    // TODO(YH, AC) add more extractions
    columns.map {
      case c@Column(name, dataType, isNullable) =>
        Row(tableCatalog, tableSchema, tableName,
                    name, c.ordinalPosition, "", isNullable.toString,
                    dataType, c.characterMaximumLength, 0,
                    c.numericPrecision, 0, 0, 0, "", "",
                    "", "", "", "")
    }
  }

  override lazy val output: Seq[Attribute] = DescribeTableFunction.output

  override def children: Seq[SparkPlan] = Seq.empty

  private case class Column(name: String, dataType: String, isNullable: Boolean) {
    def ordinalPosition: Int = 0
    def characterMaximumLength: Int = 0
    def numericPrecision: Int = 0
  }

  private implicit class LogicalPlanExtractor(val plan: LogicalPlan) {
    def schema: String = plan.schemaString

    def tableName: String = plan match {
      case Project(_, child) => child.tableName
      case Subquery(name, _) => name
      case Join(left, right, joinType, _) =>
        s"$joinType JOIN ON ${left.tableName}, ${right.tableName}"
    }

    def tableCatalog: String = ""

    def tableSchema: String = plan match {
      case p: Project => p.schemaString
    }

    def columns: Seq[Column] = plan match {
      case Project(attributes, child) => attributes match {
        case Nil =>
          Column("NO_COLUMNS", NullType.typeName, isNullable = true) :: Nil
        case list => list.map { a =>
          Column(a.name, a.dataType.typeName, a.nullable)
        }
      }
    }
  }
}

