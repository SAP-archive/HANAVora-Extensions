package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, Hierarchy}
import org.apache.spark.sql.sources.{BaseRelation, LogicalRelation, SqlLikeRelation}
import org.apache.spark.sql.types._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

// scalastyle:off magic.number
// scalastyle:off line.length

class ChangeQualifiersToTableNamesSuite extends FunSuite with MockitoSugar {

  val br1 = new BaseRelation with SqlLikeRelation {

    override def tableName: String = "testTable1"

    override def sqlContext: SQLContext = mock[SQLContext]

    override def schema: StructType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
  }

  val br2 = new BaseRelation with SqlLikeRelation {

    override def tableName: String = "testTable2"

    override def sqlContext: SQLContext = mock[SQLContext]

    override def schema: StructType = StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType)
    ))
  }

  def t1 : LogicalPlan = new LocalRelation(output = Seq(
    new AttributeReference("pred", StringType, nullable = true, metadata = Metadata.empty)(),
    new AttributeReference("succ", StringType, nullable = false, metadata = Metadata.empty)(),
    new AttributeReference("ord", StringType, nullable = false, metadata = Metadata.empty)()
  ).map(_.toAttribute)
  )

  def catalog : Catalog = {
    val catalog = new SimpleCatalog(SimpleCatalystConf(true))
    catalog.registerTable(Seq("T1"), t1)
    catalog
  }

  def analyzer : Analyzer = new Analyzer(catalog, EmptyFunctionRegistry, SimpleCatalystConf(true))

  val lr1 = LogicalRelation(br1)
  val lr2 = LogicalRelation(br2)
  val nameAtt = lr1.output.find(_.name == "name").get
  val ageAtt = lr1.output.find(_.name == "age").get

  val nameAtt2 = lr2.output.find(_.name == "name").get
  val ageAtt2 = lr2.output.find(_.name == "age").get

  val h = Hierarchy("hchy", lr1,
    "u", 'pred==='succ, SortOrder('name, Ascending) :: Nil, 'bla, 'bla)


  test("Add alias to table with where clause") {
    assertResult(lr1.subquery('table1).select(nameAtt, ageAtt).where(ageAtt <= 3)) {
      ChangeQualifiersToTableNames(lr1.select(nameAtt, ageAtt).where(ageAtt <= 3))
    }
  }

  test("Add alias to table") {

    // Simple select we only have a LogicalRelation in the logical plan
    assertResult(lr1.subquery('table1).select(nameAtt)) {
      ChangeQualifiersToTableNames(lr1.select(nameAtt))
    }

    // Two Joins different tables
    assertResult(lr1.subquery('table1).select(nameAtt)
      .join(lr2.subquery('table2).select(nameAtt2))) {
      ChangeQualifiersToTableNames(lr1.select(nameAtt).join(lr2.select(nameAtt2)))
    }

    // Join same table
    assertResult(lr1.subquery('table1).select(nameAtt)
      .join(lr1.subquery('table2).select(nameAtt))) {
      ChangeQualifiersToTableNames(lr1.select(nameAtt).join(lr1.select(nameAtt)))
    }

    // Simple select forcing two qualifiers
    val nameAttrModified = nameAtt
      .copy()(exprId = nameAtt.exprId, qualifiers = "blah" :: "bleh" :: Nil)

    intercept[RuntimeException] {
      ChangeQualifiersToTableNames(lr1.select(nameAttrModified))
    }

    // Nonexistent attribute reference
    ChangeQualifiersToTableNames(lr1.select(AttributeReference("blah", StringType)()))

    // Do not alias hierarchy source relation
    assertResult(h.select('name)) {
      ChangeQualifiersToTableNames(h.select('name))
    }
  }

}
