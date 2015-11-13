package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.types.compat._
import org.apache.spark.sql.types.Node
import org.apache.spark.Logging
import org.apache.spark.sql.Row

// scalastyle:off magic.number
class HierarchyJoinBuilderUnitTests extends NodeUnitTestSpec with Logging {
  var jb = new HierarchyJoinBuilder[Row, Row, Long](null, null, null, null, null, null)

  log.info("Testing function 'extractNodeFromRow'\n")

  val x = new Node(List(1,2,3), ordPath = List(1,1,2))
  Some(x) should equal {
    jb.extractNodeFromRow(Row.fromSeq(Seq(1,2,3, x)))
  }

  None should equal {
    jb.extractNodeFromRow(Row.fromSeq(Seq(1,2,3)))
  }

  None should equal {
    jb.extractNodeFromRow(Row.fromSeq(Seq()))
  }

  log.info("Testing function 'getOrd'\n")
   None should equal {
     jb.getOrd(Row.fromSeq(Seq(1,2,3)))
   }
  val testValues = List((42L, Some(42L)), (13, Some(13L)), ("hello", None), (1234.56, None))
  testValues.foreach(
    testVal => {
      val jbWithOrd = new HierarchyJoinBuilder[Row, Row, Long](null, null, null, null,
        x => testVal._1
        , null)
    testVal._2 should equal {
      jbWithOrd.getOrd(Row.fromSeq(Seq(x)))
    }
    }
  )
}
// scalastyle:on magic.number
