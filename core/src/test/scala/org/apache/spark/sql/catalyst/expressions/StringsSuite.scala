package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.Logging
import org.apache.spark.sql.GlobalVelocitySQLContext
import org.scalatest.FunSuite

class StringsSuite
  extends FunSuite
  with GlobalVelocitySQLContext
  with Logging {

  // scalastyle:off magic.number

  val rowA = StringRow(" AAA")
  val rowB = StringRow("BBB ")
  val rowC = StringRow(" CCC ")
  val rowD = StringRow("DDDDDDD")
  val rowE = StringRow(null)

  val dataWithDates = Seq(rowA, rowB, rowC, rowD, rowE)

  test("string manipulations") {
    val rdd = sc.parallelize(dataWithDates)
    val dSrc = sqlContext.createDataFrame(rdd).cache()
    dSrc.registerTempTable("src")

    val result1 = 
      sqlContext.sql("SELECT name,TRIM(name),RTRIM(name),LTRIM(name) FROM src").collect

    assertResult(Row(rowA.name, "AAA"," AAA","AAA") ::
      Row(rowB.name, "BBB","BBB","BBB ") ::
      Row(rowC.name, "CCC"," CCC","CCC ") ::
      Row(rowD.name, "DDDDDDD","DDDDDDD","DDDDDDD") :: 
      Row(rowE.name, null, null, null) :: Nil)(result1)

    val result2 = 
      sqlContext.sql("SELECT name,LPAD(name,6,'x'),RPAD(name,6,'xyz') FROM src").collect

    assertResult(Row(rowA.name, "xx AAA", " AAAxy") ::
      Row(rowB.name, "xxBBB ", "BBB xy") ::
      Row(rowC.name, "x CCC ", " CCC x") ::
      Row(rowD.name, "DDDDDD", "DDDDDD") :: 
      Row(rowE.name, null, null) :: Nil)(result2)

    val result3 = 
      sqlContext.sql("SELECT name, LENGTH(name), LOCATE(name,'B') FROM src").collect

    assertResult(Row(rowA.name, 4, -1) ::
      Row(rowB.name, 4, 0) ::
      Row(rowC.name, 5,-1) ::
      Row(rowD.name, 7,-1) :: 
      Row(rowE.name, 0, -1) :: Nil)(result3)

    val result4 = sqlContext.sql("SELECT name, CONCAT(name,'aa') FROM src").collect

    assertResult(Row(rowA.name, " AAAaa") ::
      Row(rowB.name, "BBB aa") ::
      Row(rowC.name, " CCC aa") ::
      Row(rowD.name, "DDDDDDDaa") ::
      Row(rowE.name, null) ::Nil)(result4)

    val result5 = 
      sqlContext.sql("SELECT name,REPLACE(name,'DD','de'),REVERSE(name) FROM src").collect

    assertResult(Row(rowA.name, " AAA", "AAA ") ::
      Row(rowB.name, "BBB ", " BBB") ::
      Row(rowC.name, " CCC ", " CCC ") ::
      Row(rowD.name, "dededeD","DDDDDDD") :: 
      Row(rowE.name, null, null) :: Nil)(result5)
  }
}

case class StringRow(name: String)

