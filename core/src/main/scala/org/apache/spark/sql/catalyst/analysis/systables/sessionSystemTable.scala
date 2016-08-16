package org.apache.spark.sql.catalyst.analysis.systables
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SQLConf, SQLContext}

/**
  * [[SystemTableProvider]] for the session system table.
  *
  * The session system table currently only exists for [[LocalSpark]].
  */
object SessionSystemTableProvider extends SystemTableProvider with LocalSpark {

  /** @inheritdoc */
  override def create(sqlContext: SQLContext): SystemTable =
    SparkLocalSessionSystemTable(sqlContext)
}

/**
  * The spark local session system table.
  *
  * This system table reports back settings from both the [[org.apache.spark.SparkContext]] and
  * the [[SQLContext]].
  *
  * @param sqlContext The Spark [[SQLContext]].
  */
case class SparkLocalSessionSystemTable(sqlContext: SQLContext)
  extends SystemTable
  with AutoScan {

  /** @inheritdoc */
  override def execute(): Seq[Row] = {
    val sqlContextSettings = allSettingsOf(sqlContext.conf)
    val sparkContextSettings = sqlContext.sparkContext.conf.getAll.toMap
    Seq(
      "SQLContext" -> sqlContextSettings,
      "SparkContext" -> sparkContextSettings).flatMap {
      case (section, values) => values.map {
        case (key, value) => Row(section, key, value)
      }
    }
  }

  /**
    * Retrieves all settings from the given [[SQLConf]] including not-overridden default values.
    *
    * @param conf The [[SQLConf]].
    * @return All settings of the given [[SQLConf]] including not-overridden default values.
    */
  private def allSettingsOf(conf: SQLConf): Map[String, String] = {
    val setConfs = conf.getAllConfs
    val defaultConfs = conf.getAllDefinedConfs.collect {
      case (key, default, _) if !setConfs.contains(key) => key -> default
    }
    setConfs ++ defaultConfs
  }

  override def schema: StructType = SessionSystemTable.schema
}

object SessionSystemTable extends SchemaEnumeration {
  val section = Field("SECTION", StringType, nullable = false)
  val key = Field("KEY", StringType, nullable = false)
  val value = Field("VALUE", StringType, nullable = true)
}
