package org.apache.spark.sql.execution.tablefunctions

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.DummyRelationUtils._

case class TPCHTables(sqlContext: SQLContext) {

  val partSchema = StructType(
    'P_PARTKEY.int ::
      'P_NAME.string ::
      'P_MFGR.string ::
      'P_BRAND.string ::
      'P_TYPE.string ::
      'P_SIZE.int ::
      'P_CONTAINER.string ::
      'P_RETAILPRICE.decimal(16,2) :: // scalastyle:ignore
      'P_COMMENT.string :: Nil)

  val nationSchema = StructType(
    'N_NATIONKEY.int ::
      'N_NAME.string ::
      'N_REGIONKEY.int ::
      'N_COMMENT.string :: Nil)

  val regionSchema = StructType(
    'R_REGIONKEY.int ::
      'R_NAME.string ::
      'R_COMMENT.string :: Nil)

  val supplierSchema = StructType(
    'S_SUPPKEY.int ::
      'S_NAME.string ::
      'S_ADDRESS.string ::
      'S_NATIONKEY.int ::
      'S_PHONE.string ::
      'S_ACCTBAL.decimal(16,2) :: // scalastyle:ignore
      'S_COMMENT.string :: Nil)

  val partsuppSchema = StructType(
    'PS_PARTKEY.int ::
      'PS_SUPPKEY.int ::
      'PS_AVAILQTY.int ::
      'PS_SUPPLYCOST.decimal(16,2) :: // scalastyle:ignore
      'PS_COMMENT.string :: Nil)

  val customerSchema = StructType(
    'C_CUSTKEY.int ::
      'C_NAME.string ::
      'C_ADDRESS.string ::
      'C_NATIONKEY.int ::
      'C_PHONE.string ::
      'C_ACCTBAL.decimal(16,2) :: // scalastyle:ignore
      'C_MKTSEGMENT.string ::
      'C_COMMENT.string :: Nil)

  val ordersSchema = StructType(
    'O_ORDERKEY.int ::
      'O_CUSTKEY.int ::
      'O_ORDERSTATUS.string ::
      'O_TOTALPRICE.decimal(16,2) :: // scalastyle:ignore
      'O_ORDERDATE.date ::
      'O_ORDERPRIORITY.string ::
      'O_CLERK.string ::
      'O_SHIPPRIORITY.int ::
      'O_COMMENT.string :: Nil)

  val lineItemSchema = StructType(
    'L_ORDERKEY.int ::
      'L_PARTKEY.int ::
      'L_SUPPKEY.int ::
      'L_LINENUMBER.int ::
      'L_QUANTITY.decimal(16,2) :: // scalastyle:ignore
      'L_EXTENDEDPRICE.decimal(16,2) :: // scalastyle:ignore
      'L_DISCOUNT.decimal(16,2) :: // scalastyle:ignore
      'L_TAX.decimal(16,2) :: // scalastyle:ignore
      'L_RETURNFLAG.string ::
      'L_LINESTATUS.string ::
      'L_SHIPDATE.date ::
      'L_COMMITDATE.date ::
      'L_RECEIPTDATE.date ::
      'L_SHIPINSTRUCT.string ::
      'L_SHIPMODE.string ::
      'L_COMMENT.string :: Nil)

  val partTable = SqlLikeDummyRelation("PART", partSchema)(sqlContext)
  val nationTable = SqlLikeDummyRelation("NATION", nationSchema)(sqlContext)
  val regionTable = SqlLikeDummyRelation("REGION", regionSchema)(sqlContext)
  val supplierTable = SqlLikeDummyRelation("SUPPLIER", supplierSchema)(sqlContext)
  val partsuppTable = SqlLikeDummyRelation("PARTSUPP", partsuppSchema)(sqlContext)
  val customerTable = SqlLikeDummyRelation("CUSTOMER", customerSchema)(sqlContext)
  val ordersTable = SqlLikeDummyRelation("ORDERS", ordersSchema)(sqlContext)
  val lineItemTable = SqlLikeDummyRelation("LINEITEM", lineItemSchema)(sqlContext)

  val all =
    Set(
      partTable,
      nationTable,
      regionTable,
      supplierTable,
      partsuppTable,
      customerTable,
      ordersTable,
      lineItemTable)
}
