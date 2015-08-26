package org.apache.spark.sql.hierarchy

case class EmployeeRow(name : String, pred : Option[Long], succ : Long, ord : Int)

case class AnimalRow(name : String, pred : Option[Long], succ : Long, ord : Long)

case class AddressRow(name : String, address : String)

case class PartialResult(path: Seq[Long], pk: Long)

/**
 * Set of data types and test data for hierarchy-related tests.
 */
// scalastyle:off magic.number
trait HierarchyTestUtils {

  protected def animalsHierarchy : Seq[AnimalRow] = Seq(
    AnimalRow("Animal", None, 1L, 1L),
    AnimalRow("Mammal", Some(1L), 2L, 1L),
    AnimalRow("Oviparous", Some(1L), 3L, 2L)
  )

  protected def organizationHierarchy : Seq[EmployeeRow] = Seq(
    EmployeeRow("THE BOSS", None, 1L, 1),
    EmployeeRow("The Middle Manager", Some(1L), 2L, 1),
    EmployeeRow("The Other Middle Manager", Some(1L), 3L, 2),
    EmployeeRow("Senior Developer", Some(2L), 4L, 1),
    EmployeeRow("Minion 1", Some(2L), 5L, 2),
    EmployeeRow("Minion 2", Some(4L), 6L, 1),
    EmployeeRow("Minion 3", Some(4L), 7L, 2)
  )

  protected def addresses : Seq[AddressRow] = Seq(
    AddressRow("THE BOSS", "Nice Street"),
    AddressRow("The Middle Manager", "Acceptable Street"),
    AddressRow("Senior Developer", "Near-Acceptable Street"),
    AddressRow("Minion 3", "The Street"),
    AddressRow("Darth Vader", "Death Star")
  )
}
