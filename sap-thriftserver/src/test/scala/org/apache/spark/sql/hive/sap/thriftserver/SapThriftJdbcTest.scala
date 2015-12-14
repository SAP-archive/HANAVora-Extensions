package org.apache.spark.sql.hive.sap.thriftserver

import java.sql.{DriverManager, Statement}

import org.apache.hive.jdbc.HiveDriver
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * Each test with a different driver can inherit from this abstract class.
 *
 * This class assumes that it gets a running thriftserver!
 *
 * @param thriftServer
 */
abstract class SapThriftJdbcTest(val thriftServer: SapThriftServer2Test){

  def jdbcUri: String

  def withMultipleConnectionJdbcStatement(fs: (Statement => Unit)*) {
    val user = System.getProperty("user.name")
    val connections = fs.map { _ => DriverManager.getConnection(jdbcUri, user, "") }
    val statements = connections.map(_.createStatement())

    try {
      statements.zip(fs).foreach { case (s, f) => f(s) }
    } finally {
      statements.foreach(_.close())
      connections.foreach(_.close())
    }
  }

  def withJdbcStatement(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(f)
  }

}

class SapThriftJdbcHiveDriverTest(override val thriftServer: SapThriftServer2Test)
  extends SapThriftJdbcTest(thriftServer) {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  override def jdbcUri: String = if (thriftServer.mode == ServerMode.http) {
    s"""jdbc:hive2://${thriftServer.getServerAdressAndPort()}/
        |default?
        |hive.server2.transport.mode=http;
        |hive.server2.thrift.http.path=cliservice
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://${thriftServer.getServerAdressAndPort()}/"
  }

}
