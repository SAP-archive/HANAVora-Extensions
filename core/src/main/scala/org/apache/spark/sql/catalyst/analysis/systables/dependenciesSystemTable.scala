package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.catalyst.analysis.TableDependencyCalculator
import org.apache.spark.sql.sources.{RelationKind, Table}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

object DependenciesSystemTableProvider extends SystemTableProvider with LocalSpark {
  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable = DependenciesSystemTable(sqlContext)
}

sealed trait DependencyType extends Product {
  /** ID of this [[DependencyType]] */
  val id: Int
}

sealed abstract class BaseDependencyType(val id: Int) extends DependencyType

case object ReferenceDependency extends BaseDependencyType(0)

/**
  * A [[SystemTable]] that calculates the dependencies between Spark catalog items.
  *
  * @param sqlContext The Spark [[SQLContext]].
  */
case class DependenciesSystemTable(sqlContext: SQLContext)
  extends SystemTable
  with TableDependencyCalculator
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val tables = getTables(sqlContext.catalog)
    val dependentsMap = buildDependentsMap(tables, sqlContext)

    dependentsMap.flatMap {
      case (tableIdent, dependents) =>
        val curTable = tables(tableIdent)
        val curKind = RelationKind.kindOf(curTable, Table)
        dependents.map { dependent =>
          val dependentTable = tables(dependent)
          val dependentKind = RelationKind.kindOf(dependentTable, Table)
          Row(
            tableIdent.database.orNull,
            tableIdent.table,
            curKind.name.toUpperCase,
            dependent.database.orNull,
            dependent.table,
            dependentKind.name.toUpperCase,
            ReferenceDependency.id)
        }
    }.toSeq
  }

  override val schema: StructType =
    StructType(
      Seq(
        StructField("BASE_SCHEMA_NAME", StringType, nullable = true),
        StructField("BASE_OBJECT_NAME", StringType, nullable = false),
        StructField("BASE_OBJECT_TYPE", StringType, nullable = false),
        StructField("DEPENDENT_SCHEMA_NAME", StringType, nullable = true),
        StructField("DEPENDENT_OBJECT_NAME", StringType, nullable = false),
        StructField("DEPENDENT_OBJECT_TYPE", StringType, nullable = false),
        StructField("DEPENDENCY_TYPE", IntegerType, nullable = false)))
}
