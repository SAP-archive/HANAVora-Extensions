package org.apache.spark.sql.hive

import com.sap.spark.GlobalSparkContext
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.Utils
import org.scalatest.FunSuite

class HiveCatalogTestSuite extends FunSuite with GlobalSparkContext {

  /**
    * This test creates a hive table and makes sure it is not visible in the SAPHiveContext
    */
  test("Hive Catalog is separated") {
    val sparkHiveContext = new TestHiveContext(sc)
    // The hive system requires to set that variable we set it here and unset it in the end
    // see org.apache.hadoop.hive.ql.hooks.EnforeReadOnlyTables class
    System.setProperty("test.src.tables", "abc")
    sparkHiveContext.sql("CREATE TABLE xyz (key INT, value STRING)")

    val tablesInHiveContext = sparkHiveContext.catalog.getTables(None)
    // table "xyz" should be in the Spark HiveContext
    assert(tablesInHiveContext.contains(("xyz", false)))

    // Here we create an SAPHiveContext based on the same metastore and Hive configuration as the
    // Spark one. If we do not do so and create a new SapiveContext it will create a new temporary
    // metastore and we cannot test if tables are hidden. The underlying Hive system contains table
    // "xyz" but the SAPHiveContext
    // should hide table "xyz"
    val sapHiveContext = new SapHiveContext(sc, sparkHiveContext.cacheManager,
      sparkHiveContext.listener, sparkHiveContext.executionHive,
      sparkHiveContext.metadataHive, false)

    // and there it shouldn't be in
    assert(sapHiveContext.sql("show tables").collect().length == 0)
    System.clearProperty("test.src.tables")
  }

  /**
    * This class is for testing purposes: In contrast to the standard hive context the metastore
    * directory is set as temporary directory such that parallel tests do not clash.
    * @param sc
    */
  class TestHiveContext(sc: SparkContext) extends HiveContext(sc) {
    self =>

    // has to be set to lazy otherwise a null pointer exception is thrown
    lazy val warehousePath = Utils.createTempDir(namePrefix = "warehouse-")

    /** Sets up the system initially or after a RESET command */
    protected override def configure(): Map[String, String] = {
      super.configure() ++ Map(
        ConfVars.METASTOREWAREHOUSE.varname -> warehousePath.toURI.toString
      )
    }
  }

}
