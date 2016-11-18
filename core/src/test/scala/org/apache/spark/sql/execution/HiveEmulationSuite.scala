package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.hive.SapHiveContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, GlobalSapSQLContext, Row, SapSQLConf}
import org.apache.spark.util.DummyRelationUtils._
import org.apache.spark.util.SqlContextConfigurationUtils
import org.scalatest.FunSuite

class HiveEmulationSuite
  extends FunSuite
  with GlobalSapSQLContext
  with SqlContextConfigurationUtils {

  private def createTable(name: String, schema: StructType): Unit =
    sqlc.createDataFrame(sc.parallelize(Seq.empty[Row]), schema).registerTempTable(name)

  private def withHiveEmulation[A](op: => A): A =
    withConf(SapSQLConf.HIVE_EMULATION.key, "true")(op)

  test("Show schemas shows a default schema when hive emulation is on") {
    withHiveEmulation {
      val values = sqlc.sql("SHOW SCHEMAS").collect().toSet

      assertResult(Set(Row("default")))(values)
    }
  }

  test("Show schemas throws if hive emulation is off") {
    intercept[RuntimeException](sqlc.sql("SHOW SCHEMAS"))
  }

  test("Desc an existing table") {
    withHiveEmulation {
      createTable("foo", StructType('a.int :: 'b.int :: Nil))
      val values = sqlc.sql("DESC foo").collect().toSet

      assertResult(
        Set(
          Row("a", "int", null),
          Row("b", "int", null)))(values)
    }
  }

  test("Desc a non-existent table throws") {
    withHiveEmulation {
      intercept[NoSuchTableException] {
        sqlc.sql("DESC bar").collect()
      }
    }
  }

  test("Describe an existing table") {
    withHiveEmulation {
      createTable("foo", StructType('a.int :: 'b.int :: Nil))
      val values = sqlc.sql("DESCRIBE FORMATTED foo").collect().toList
      assertResult(
        List(
          Row(s"# col_name${" " * 12}\tdata_type${" " * 11}\tcomment${" " * 13}\t"),
          Row(""),
          Row(s"a${" " * 19}\tint${" " * 17}\tnull${" " * 16}\t"),
          Row(s"b${" " * 19}\tint${" " * 17}\tnull${" " * 16}\t")))(values)
    }
  }

  test("Retrieval of a database prefixed table") {
    val hc = new SapHiveContext(sc)
    hc.setConf(SapSQLConf.HIVE_EMULATION, true)
    val expected = Set(Row(0, 0), Row(0, 1), Row(1, 0), Row(1, 1))
    val rdd = hc.sparkContext.parallelize(expected.toSeq)
    hc.createDataFrame(rdd, StructType('a.int :: 'b.int :: Nil)).registerTempTable("foo")

    val results = hc.sql("SELECT * FROM default.foo").collect().toSet
    assertResult(expected)(results)

    hc.setConf(SapSQLConf.HIVE_EMULATION, false)
    intercept[AnalysisException] {
      hc.sql("SELECT * FROM default.foo")
    }
  }

  test("USE statements should not do anything when in hive emulation mode") {
    withConf(SapSQLConf.HIVE_EMULATION.key, "true") {
      sqlc.sql("USE foo bar")
    }
  }

  test("Any other use command should throw an exception") {
    intercept[RuntimeException] {
      sqlc.sql("USE foo bar")
    }
  }
}
