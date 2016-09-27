package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.catalyst.TableIdentifier
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
    val dependentsMap = buildDependentsMap(tables)

    def kindOf(tableIdentifier: TableIdentifier): String =
      tables
        .get(tableIdentifier)
        .map(plan => RelationKind.kindOf(plan).getOrElse(Table).name)
        .getOrElse(DependenciesSystemTable.UnknownType)
        .toUpperCase

    dependentsMap.flatMap {
      case (tableIdent, dependents) =>
        val curKind = kindOf(tableIdent)
        dependents.map { dependent =>
          val dependentKind = kindOf(dependent)
          Row(
            tableIdent.database.orNull,
            tableIdent.table,
            curKind,
            dependent.database.orNull,
            dependent.table,
            dependentKind,
            ReferenceDependency.id)
        }
    }.toSeq
  }

  override val schema: StructType = DependenciesSystemTable.schema
}

object DependenciesSystemTable extends SchemaEnumeration {
  val baseSchemaName = Field("BASE_SCHEMA_NAME", StringType, nullable = true)
  val baseObjectName = Field("BASE_OBJECT_NAME", StringType, nullable = false)
  val baseObjectType = Field("BASE_OBJECT_TYPE", StringType, nullable = false)
  val dependentSchemaName = Field("DEPENDENT_SCHEMA_NAME", StringType, nullable = true)
  val dependentObjectName = Field("DEPENDENT_OBJECT_NAME", StringType, nullable = false)
  val dependentObjectType = Field("DEPENDENT_OBJECT_TYPE", StringType, nullable = false)
  val dependencyType = Field("DEPENDENCY_TYPE", IntegerType, nullable = false)

  private[DependenciesSystemTable] val UnknownType = "UNKNOWN"
}
