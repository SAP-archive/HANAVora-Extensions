package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.SQLContext
import scala.reflect.ClassTag
import scala.util.Random
import scala.reflect.runtime.universe.TypeTag

/**
 * Set of data types and test data for hierarchy-related tests.
 */
// scalastyle:off magic.number
trait HierarchyTestUtils {

  protected def animalsHierarchy: Seq[AnimalRow] = Seq(
    AnimalRow("Animal", None, 1L, 1L),
    AnimalRow("Mammal", Some(1L), 2L, 1L),
    AnimalRow("Oviparous", Some(1L), 3L, 2L),
    AnimalRow("Carnivores", Some(2L), 4L, 3L),
    AnimalRow("Herbivores", Some(2L), 5L, 4L)

  )

  /*  --------------------------------------------
      - The hierarchy with correct sibling order -
      --------------------------------------------
                          The Boss(prerank: 1)
                             /          \
                           MM(2)       OMM(7)
                          /     \
                  SeniorDev(3)  Minion1(6)
                  /           \
            Minion2(4)        Minion3(5)
   */
  protected def organizationHierarchy: Seq[EmployeeRow] = Seq(
    EmployeeRow("THE BOSS", None, 1L, 1),
    EmployeeRow("The Middle Manager", Some(1L), 2L, 1),
    EmployeeRow("The Other Middle Manager", Some(1L), 3L, 2),
    EmployeeRow("Senior Developer", Some(2L), 4L, 1),
    EmployeeRow("Minion 1", Some(2L), 5L, 2),
    EmployeeRow("Minion 2", Some(4L), 6L, 1),
    EmployeeRow("Minion 3", Some(4L), 7L, 2)
  )

  protected def addresses: Seq[AddressRow] = Seq(
    AddressRow("THE BOSS", "Nice Street"),
    AddressRow("The Middle Manager", "Acceptable Street"),
    AddressRow("Senior Developer", "Near-Acceptable Street"),
    AddressRow("Minion 3", "The Street"),
    AddressRow("Darth Vader", "Death Star")
  )

  protected def sensors: Seq[SensorRow] = Seq(
    SensorRow("c", "", "All Sensors"),
    SensorRow("c.1", "c", "A Model"),
    SensorRow("c.2", "c", "B Model"),
    SensorRow("c.3", "c", "C Model"),
    SensorRow("c.1.1", "c.1", "A Model 1"),
    SensorRow("c.1.2", "c.1", "A Model 2"),
    SensorRow("c.1.3", "c.1", "A Model 3"),
    SensorRow("c.2.1", "c.2", "B Model 1"),
    SensorRow("c.2.2", "c.2", "B Model 2"),
    SensorRow("c.2.3", "c.2", "B Model 3"),
    SensorRow("c.3.1", "c.3", "C Model 1"),
    SensorRow("c.3.2", "c.3", "C Model 2"),
    SensorRow("c.3.3", "c.3", "C Model 3")
  )

  protected def parts: Seq[ComponentRow] = Seq(
    ComponentRow("bla", "mat-for-stuff", 0L, 1L, 1L),
    ComponentRow("bla", "item-a-gen", 1L, 2L, 2L),
    ComponentRow("bla", "item-o-piece", 2L, 3L, 3L),
    ComponentRow("bla", "object-for-entity", 3L, 4L, 4L),
    ComponentRow("bla", "whack-to-piece", 3L, 5L, 5L),
    ComponentRow("bla", "gen-a-stuff", 3L, 6L, 6L),
    ComponentRow("bla", "mat-with-whack", 5L, 7L, 7L)
  )

  def orgTbl: String = "organizationTbl"
  def addressesTable: String = "addressesTbl"
  def sensorsTable: String = "sensorTbl"
  def partsTable: String = "partsTbl"
  def animalsTable: String = "animalsTbl"

  def createOrgTable(sc: SQLContext): Unit = {
    createTable(sc, organizationHierarchy, orgTbl)
  }

  def createAddressesTable(sc: SQLContext): Unit = {
    createTable(sc, addresses, addressesTable)
  }

  def createSensorsTable(sc: SQLContext): Unit = {
    createTable(sc, sensors, sensorsTable)
  }

  def createPartsTable(sc: SQLContext): Unit = {
    createTable(sc, parts, partsTable)
  }

  def createAnimalsTable(sc: SQLContext): Unit = {
    createTable(sc, animalsHierarchy, animalsTable)
  }

  private[this] def createTable[A <: Product: TypeTag: ClassTag](sc: SQLContext,
                                                      seq: Seq[A],
                                                      name: String): Unit = {
    val rdd = sc.sparkContext.parallelize[A](seq.sortBy
      (x => Random.nextDouble()))(implicitly[ClassTag[A]])
    sc.createDataFrame[A](rdd)(implicitly[TypeTag[A]]).cache().registerTempTable(name)
  }

  def hierarchySQL(table: String, projectionColumns: String = "*"): String = {
    s"""|(SELECT $projectionColumns
        | FROM HIERARCHY
        | (USING $table AS v JOIN PARENT u ON v.pred = u.succ
        | SEARCH BY ord ASC
        | START WHERE pred IS NULL
        | SET node) AS H)""".stripMargin
  }
}
