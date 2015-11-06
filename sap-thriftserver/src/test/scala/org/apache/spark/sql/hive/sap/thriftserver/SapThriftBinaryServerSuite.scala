package org.apache.spark.sql.hive.sap.thriftserver

import java.sql.ResultSet

import com.sap.spark.util.TestUtils._
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli.GetInfoType
import org.apache.hive.service.cli.thrift.TCLIService.Client
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient
import org.apache.spark.Logging
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import scala.collection.mutable.ListBuffer

class SapThriftBinaryServerSuite extends SapThriftJdbcTest2 with Logging {
  override def mode: ServerMode.Value = ServerMode.binary

  val filePath = getFileFromClassPath("/simpleData.json")

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    withJdbcStatement { statement =>
      val queries = Seq(
        s"""CREATE TEMPORARY TABLE $tableName
            |USING org.apache.spark.sql.json
            |OPTIONS (path "$filePath")""".stripMargin)

      queries.foreach(statement.execute)
      logInfo("Test table is created.")
    }
  }

  private def withCLIServiceClient(f: ThriftCLIServiceClient => Unit): Unit = {
    // Transport creation logics below mimics HiveConnection.createBinaryTransport
    val rawTransport = new TSocket("localhost", serverPort)
    val user = System.getProperty("user.name")
    val transport = PlainSaslHelper.getPlainTransport(user, "anonymous", rawTransport)
    val protocol = new TBinaryProtocol(transport)
    val client = new ThriftCLIServiceClient(new Client(protocol))

    transport.open()
    try f(client) finally transport.close()
  }

  test("GetInfo Thrift API") {
    withCLIServiceClient { client =>
      val user = System.getProperty("user.name")
      val sessionHandle = client.openSession(user, "")

      assertResult("Spark SQL", "Wrong GetInfo(CLI_DBMS_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME).getStringValue
      }

      assertResult("Spark SQL", "Wrong GetInfo(CLI_SERVER_NAME) result") {
        client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME).getStringValue
      }

      assertResult(true, "Spark version shouldn't be \"Unknown\"") {
        val version = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER).getStringValue
        logInfo(s"Spark version: $version")
        version != "Unknown"
      }
    }
  }

  def resultSetTolist(rs: ResultSet): List[(Any, Any)] = {

    val values = new ListBuffer[(Any, Any)]
    while (rs.next())
      values += new Tuple2(rs.getString(2), rs.getInt(1))
    values.toList
  }



  // scalastyle:off magic.number
  test("JDBC query execution") {
    withJdbcStatement { statement =>
      assertResult(4, "Row count mismatch") {
        val resultSet = statement.executeQuery(s"""SELECT COUNT(*) FROM $tableName""")
        resultSet.next()
        resultSet.getInt(1)
      }
    }
  }
  // scalastyle:on magic.number
  // scalastyle:off magic.number
  test("Simple select query no params") {
    withJdbcStatement { statement =>
      val resultSet = statement.executeQuery(s"""SELECT * FROM $tableName""")

      // Checking result size: 4
      val results = resultSetTolist(resultSet)

      assert(results contains("hans", 10))
      assert(results contains("peter", 20))
      assert(results contains("hans", 20))
      assert(results contains("peter", 40))
    }
  }
  // scalastyle:on magic.number

}

object ServerMode extends Enumeration {
  val binary, http = Value
}
