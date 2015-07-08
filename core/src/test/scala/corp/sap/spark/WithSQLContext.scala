package corp.sap.spark

import org.apache.spark.sql.SQLContext
import org.scalatest.{BeforeAndAfterEach, Suite}

trait WithSQLContext extends BeforeAndAfterEach {
  self: Suite with WithSparkContext =>

  override def beforeEach(): Unit = {
    try {
      super.beforeEach()
      setUpSQLContext()
    } catch {
      case ex: Throwable =>
        tearDownSQLContext()
        throw ex
    }
  }

  override def afterEach(): Unit = {
    try {
      super.afterEach()
    } finally {
      tearDownSQLContext()
    }
  }

  implicit def sqlContext: SQLContext = _sqlContext
  def sqlc: SQLContext = sqlContext

  var _sqlContext: SQLContext = _

  protected def setUpSQLContext(): Unit =
    _sqlContext = new SQLContext(sc)


  protected def tearDownSQLContext(): Unit =
    _sqlContext = null

}
