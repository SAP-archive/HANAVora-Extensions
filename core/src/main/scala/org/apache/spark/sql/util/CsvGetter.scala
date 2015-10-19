package org.apache.spark.sql.util

object CsvGetter {

  def getFileFromClassPath(fileName: String): String = {
    getClass().getResource(fileName).getPath.replaceAll("/C:", "")
  }

}
