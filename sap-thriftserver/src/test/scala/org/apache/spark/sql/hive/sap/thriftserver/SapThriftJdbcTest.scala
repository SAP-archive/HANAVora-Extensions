package org.apache.spark.sql.hive.sap.thriftserver

import java.sql.{DriverManager, Statement}

import org.apache.hive.jdbc.HiveDriver

/**
 * Each test with a different driver can inherit from this abstract class. The master parameter
 * is passed on the the SapThriftServer2Test
 *
 * @param master
 */
abstract class SapThriftJdbcTest(override val master: String) extends SapThriftServer2Test(master) {

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

  protected def withJdbcStatement(f: Statement => Unit): Unit = {
    withMultipleConnectionJdbcStatement(f)
  }
}

class SapThriftJdbcHiveDriverTest(override val master: String) extends SapThriftJdbcTest(master) {
  Class.forName(classOf[HiveDriver].getCanonicalName)

  override def jdbcUri: String = if (mode == ServerMode.http) {
    s"""jdbc:hive2://localhost:$serverPort/
        |default?
        |hive.server2.transport.mode=http;
        |hive.server2.thrift.http.path=cliservice
     """.stripMargin.split("\n").mkString.trim
  } else {
    s"jdbc:hive2://localhost:$serverPort/"
  }

  override def mode: ServerMode.Value = ServerMode.binary

}
