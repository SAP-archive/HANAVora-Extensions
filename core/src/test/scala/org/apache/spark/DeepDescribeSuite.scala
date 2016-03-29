package org.apache.spark

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.sources.describable.{DefaultLogicalPlanDescriber, LogicalPlanDescribable, StructureDescriber}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{GlobalSapSQLContext, Row}
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

case class Person(name: String, age: Int)

class DeepDescribeSuite extends FunSuite with GlobalSapSQLContext with MockitoSugar {
  // scalastyle:off magic.number
  val persons = Person("John", 30) :: Person("Bill", 40) :: Nil
  // scalastyle:on magic.number

  implicit class Description(symbol: Symbol) {
    def ~>[A](a: A): A = a // scalastyle:ignore
  }

  case object Dummy extends LeafNode {
    override def output: Seq[Attribute] = Nil
  }

  object CustomDescriber extends LogicalPlanDescribable {
    override val plan: LogicalPlan = Project(Nil, Dummy)

    override def additionalValues: Seq[Any] = "bar" :: Nil

    override def additionalFields: Seq[StructField] = StructField("foo", StringType) :: Nil
  }

  val plan =
    Subquery("bla",
      Project(Nil,
        LocalRelation(AttributeReference("bla", StringType)())))

  test("describe statement output") {
    sqlc.createDataFrame(persons).registerTempTable("persons")

    val values = sqlc.sql("DEEP DESCRIBE persons").collect()

    assertResult(
      Array(Row(
        'name ~> "Subquery",
        'fields ~> Seq(
          Row('name ~> "name", 'dataType ~> "string"),
          Row('name ~> "age", 'dataType ~> "integer")
        ),
        'children ~> Row(
          'child_0 ~> Row(
            'name ~> "LocalRelation",
            'fields ~> Seq(
              Row('name ~> "name", 'dataType ~> "string"),
              Row('name ~> "age", 'dataType ~> "integer")
            )
          )
        )
      )
    ))(values)
  }

  test("describe view") {
    sqlc.createDataFrame(persons).registerTempTable("persons")
    sqlc.sql("CREATE VIEW v AS SELECT * FROM persons")

    val values = sqlc.sql("DEEP DESCRIBE v").collect()

    assertResult(
      Array(
        Row(
          'name ~> "Subquery",
          'fields ~> Seq(
            Row('name ~> "name", 'dataType ~> "string"),
            Row('name ~> "age", 'dataType ~> "integer")
          ),
          'children ~> Row(
            'child_0 ~> Row(
              'name ~> "Project",
              'fields ~> Seq(
                Row('name ~> "name", 'dataType ~> "string"),
                Row('name ~> "age", 'dataType ~> "integer")
              ),
              'children ~> Row(
                'child_0 ~> Row(
                  'name ~> "Subquery",
                  'fields ~> Seq(
                    Row('name ~> "name", 'dataType ~> "string"),
                    Row('name ~> "age", 'dataType ~> "integer")
                  ),
                  'children ~> Row(
                    'child_0 ~> Row(
                      'name ~> "LocalRelation",
                      'fields ~> Seq(
                        Row('name ~> "name", 'dataType ~> "string"),
                        Row('name ~> "age", 'dataType ~> "integer")
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )
    )(values)
  }

  test("Nested structure is described correctly") {
    val desc = DefaultLogicalPlanDescriber(plan).describe()

    assertResult(Row(
      'name ~> "Subquery",
      'children ~> Row(
        'child_0 ~> Row(
          'name ~> "Project",
          'children ~> Row(
            'child_0 ~> Row(
              'name ~> "LocalRelation",
              'fields ~> Seq(
                Row(
                  'name ~> "bla",
                  'dataType ~> "string"
                )
              )
            )
          )
        )
      )
    ))(desc)
  }

  test("Nested structure schema correct") {
    val desc = DefaultLogicalPlanDescriber(plan).describeOutput

    assertResult(StructType(
      StructField("name", StringType, nullable = false) ::
      StructField("children", StructType(
        StructField("child_0", StructType(
          StructField("name", StringType, nullable = false) ::
          StructField("children", StructType(
            StructField("child_0", StructType(
              StructField("name", StringType, nullable = false) ::
              StructField("fields", ArrayType(StructType(
                StructField("name", StringType, nullable = false) ::
                StructField("dataType", StringType, nullable = false) :: Nil
              ))) :: Nil
            )) :: Nil
          )) :: Nil
        )) :: Nil
      )) :: Nil
    ))(desc)
  }

  test("DefaultLogicalPlanDescriber describes correctly") {
    val description = DefaultLogicalPlanDescriber(Project(Nil, Dummy)).describe()

    assertResult(Row(
      'name ~> "Project",
      'children ~> Row(
        'child_0 ~> Row(
          'name ~> "Dummy$"
        )
      )
    ))(description)
  }

  test("DefaultLogicalPlanDescriber calculates its schema correctly") {
    assertResult(StructType(
      LogicalPlanDescribable.defaultFields :+
      StructField("children", StructType(
        Seq(StructField("child_0", StructType(LogicalPlanDescribable.defaultFields)))
      ))
    ))(DefaultLogicalPlanDescriber(Project(Nil, Dummy)).describeOutput)
  }

  test("LogicalPlanDescribable with additional fields") {
    assertResult(StructType(
      LogicalPlanDescribable.defaultFields :+
        StructField("children", StructType(
          Seq(StructField("child_0", StructType(LogicalPlanDescribable.defaultFields)))
        )) :+
        StructField("foo", StringType)
    ))(CustomDescriber.describeOutput)
  }

  test("StructureDescriber describes correctly") {
    val structDescriber =
      StructureDescriber(
        StructField("foo", StringType) :: StructField("bar", IntegerType) :: Nil)

    val description = structDescriber.describe()

    assertResult(Row("foo", "string") :: Row("bar", "integer") :: Nil)(description)
  }
}
