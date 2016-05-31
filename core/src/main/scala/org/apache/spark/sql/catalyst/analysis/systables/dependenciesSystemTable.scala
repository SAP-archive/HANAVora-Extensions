package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.catalyst.analysis.TableDependencyCalculator
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.sources.commands.RelationKind
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object DependenciesSystemTableProvider extends SystemTableProvider with LocalSpark {
  /** @inheritdoc */
  override def create(): SystemTable = DependenciesSystemTable
}

case object DependenciesSystemTable extends SystemTable with TableDependencyCalculator {
  sealed trait DependencyType extends Product {
    /** ID of this [[DependencyType]] */
    val id: Int
  }

  sealed abstract class BaseDependencyType(val id: Int) extends DependencyType

  case object ReferenceDependency extends BaseDependencyType(0)

  /** @inheritdoc */
  override def execute(sqlContext: SQLContext): Seq[Row] = {
    val tables = getTables(sqlContext.catalog)
    val dependentsMap = buildDependentsMap(tables)

    dependentsMap.flatMap {
      case (tableIdent, dependents) =>
        val curTable = tables(tableIdent)
        val curKind = RelationKind.typeOf(curTable)
        dependents.map { dependent =>
          val dependentTable = tables(dependent)
          val dependentKind = RelationKind.typeOf(dependentTable)
          Row(
            tableIdent.database.orNull,
            tableIdent.table,
            curKind.toUpperCase,
            dependent.database.orNull,
            dependent.table,
            dependentKind.toUpperCase,
            ReferenceDependency.id)
        }
    }.toSeq
  }

  override val output: Seq[Attribute] =
    StructType(
      Seq(
        StructField("BASE_SCHEMA_NAME", StringType, nullable = true),
        StructField("BASE_OBJECT_NAME", StringType, nullable = false),
        StructField("BASE_OBJECT_TYPE", StringType, nullable = false),
        StructField("DEPENDENT_SCHEMA_NAME", StringType, nullable = true),
        StructField("DEPENDENT_OBJECT_NAME", StringType, nullable = false),
        StructField("DEPENDENT_OBJECT_TYPE", StringType, nullable = false),
        StructField("DEPENDENCY_TYPE", IntegerType, nullable = false))).toAttributes
}
