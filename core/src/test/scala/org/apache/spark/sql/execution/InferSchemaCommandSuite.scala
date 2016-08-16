package org.apache.spark.sql.execution

import java.io.FileNotFoundException

import org.apache.spark.sql.{AnalysisException, GlobalSapSQLContext, Row}
import org.scalatest.FunSuite
import com.sap.spark.util.TestUtils.{getFileFromClassPath, withTempDirectory}

class InferSchemaCommandSuite extends FunSuite with GlobalSapSQLContext {
  test("Inferring of schema fails on non-existent file") {
    withTempDirectory { dir =>
      val nonExistentPath = dir.path + "/non-existent"

      intercept[FileNotFoundException] {
        sqlc.sql(s"""INFER SCHEMA OF "$nonExistentPath" AS ORC""").collect()
      }
    }
  }

  // scalastyle:off magic.number
  test("Inferring of schema works on parquet file") {
    val personFile = getFileFromClassPath("/pers.parquet")

    val result = sqlc.sql(s"""INFER SCHEMA OF "$personFile"""").collect().toSet

    assertResult(
      Set(
        Row("name", 1, true, "VARCHAR(*)", null, null, null),
        Row("age", 2, true, "INTEGER", 32, 2, 0)))(result)
  }

  test("Inferring of schema works on orc file") {
    val personFile = getFileFromClassPath("/pers.orc")

    val result = sqlc.sql(s"""INFER SCHEMA OF "$personFile"""").collect().toSet

    assertResult(
      Set(
        Row("name", 1, true, "VARCHAR(*)", null, null, null),
        Row("age", 2, true, "INTEGER", 32, 2, 0)))(result)
  }
  // scalastyle:on magic.number

  test("Inferring of schema fails on invalid file") {
    val invalidFile = getFileFromClassPath("/simple.csv")

    intercept[AnalysisException] {
      sqlc.sql(s"""INFER SCHEMA OF "$invalidFile"""")
    }
  }
}
