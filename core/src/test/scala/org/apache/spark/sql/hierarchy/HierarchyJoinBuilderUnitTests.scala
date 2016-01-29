package org.apache.spark.sql.hierarchy

import org.apache.spark.sql.types.{IntegerType, Node}
import org.apache.spark.Logging
import org.apache.spark.sql.Row

// scalastyle:off magic.number
class HierarchyJoinBuilderUnitTests extends NodeUnitTestSpec with Logging {
  var jb = new HierarchyJoinBuilder[Row, Row, Long](null, null, null, null, null, null)

  log.info("Testing function 'extractNodeFromRow'\n")

  val x = Node(List(1,2,3), IntegerType, List(1L,1L,2L))
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
