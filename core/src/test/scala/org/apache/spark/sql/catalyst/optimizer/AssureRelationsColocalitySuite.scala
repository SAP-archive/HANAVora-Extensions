package org.apache.spark.sql.catalyst.optimizer

import com.sap.spark.PlanTest
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.{RightOuter, FullOuter, LeftOuter, Inner}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, PartitionedRelation}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{GlobalSapSQLContext, SQLContext}
import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.time.SpanSugar._


class AssureRelationsColocalitySuite
  extends FunSuite
  with GlobalSapSQLContext
  with PlanTest
  with Timeouts {

  object Optimize extends RuleExecutor[LogicalPlan] {
    private val MAX_ITERATIONS = 100

    val batches =
      Batch("RelationsColocalityAssurance", FixedPoint(MAX_ITERATIONS),
        AssureRelationsColocality) :: Nil
  }

  // Test relations
  val t0 = new LogicalRelation(new BaseRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id0", IntegerType)))
  })
  val t1 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id1", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = None
    override def partitioningFunctionName: Option[String] = None
  })
  val t2 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id2", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id2"))
    override def partitioningFunctionName: Option[String] = Some("F1")
  })
  val t3 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id3", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id3"))
    override def partitioningFunctionName: Option[String] = Some("F1")
  })
  val t4 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id4", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = None
    override def partitioningFunctionName: Option[String] = None
  })
  val t5 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id5", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id5"))
    override def partitioningFunctionName: Option[String] = Some("F2")
  })
  val t6 = new LogicalRelation(new BaseRelation with PartitionedRelation {
    override def sqlContext: SQLContext = sqlc
    override def schema: StructType = StructType(Seq(StructField("id6", IntegerType)))
    override def partitioningFunctionColumns: Option[Set[String]] = Some(Set("id6"))
    override def partitioningFunctionName: Option[String] = Some("F2")
  })

  // Join attributes
  val id0 = t0.output.find(_.name == "id0").get
  val id1 = t1.output.find(_.name == "id1").get
  val id2 = t2.output.find(_.name == "id2").get
  val id3 = t3.output.find(_.name == "id3").get
  val id4 = t4.output.find(_.name == "id4").get
  val id5 = t5.output.find(_.name == "id5").get
  val id6 = t6.output.find(_.name == "id6").get

  /**
   * This test checks the following re-orderings:
   *
   * Phase 1:
   *        O                    O
   *      /  \                 /  \
   *     O   T3(F)   ===>     T1   O
   *   /  \                      /  \
   *  T1   T2(F)             T2(F)  T3(F)
   *
   * Phase 2:
   *         O                   O
   *       /  \                /  \
   *      O   T3(F)  ===>     T1   O
   *    /  \                     /  \
   * T2(F)  T1               T2(F)  T3(F)
   *
   * Phase 3: check whether for outer joins the plan remains unchanged
   *
   * Legend:
   * T1 - unpartitioned table
   * T2, T3 - two tables partitioned by the same function F
   */
  test("Right rotation on logical plans is correctly applied in case of local co-locality") {
    // Phase 1
    val originalAnalyzedQuery1 = t1
      .join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = t2.join(t1, joinType = Inner, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = t2.join(t1, joinType = LeftOuter, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = t2.join(t1, joinType = RightOuter, condition = Some(id2 === id1))
      .join(t3, joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = t2.join(t1, joinType = FullOuter, condition = Some(id2 === id1))
      .join(t3, joinType = RightOuter, condition = Some(id2 === id3)).analyze

    val optimized5 = Optimize.execute(originalAnalyzedQuery5)

    // The plan should remain unchanged due to the outer joins
    comparePlans(optimized5, originalAnalyzedQuery5)
  }

  /**
   * This test checks the following re-orderings:
   *
   * Phase 1:
   *           O                    O
   *         /  \                 /  \
   *     T3(F)  O       ===>     O   T1
   *          /  \             /  \
   *      T2(F)  T1        T3(F)  T2(F)
   *
   * Phase 2:
   *           O                    O
   *         /  \                 /  \
   *     T3(F)  O       ===>     O   T1
   *          /  \             /  \
   *        T1   T2(F)     T3(F)  T2(F)
   *
   * Phase 3: check whether for outer joins the plan remains unchanged
   *
   * Legend:
   * T1 - unpartitioned table
   * T2, T3 - two tables partitioned by the same function F
   */
  test("Left rotation on logical plans is correctly applied in case of local co-locality") {
    // Phase 1
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t1, joinType = Inner, condition = Some(id2 === id1)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id3 === id2))
      .join(t1, joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized1, correctAnswer1)

    // Phase 2 - Inner joins
    val originalAnalyzedQuery2 = t3.join(t1
      .join(t2, joinType = Inner, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t3.join(t2, joinType = Inner, condition = Some(id3 === id2))
      .join(t1, joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Phase 2 - Outer joins
    val originalAnalyzedQuery3 = t3.join(
      t1.join(t2, joinType = FullOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    // The plan should remain unchanged due to the full outer join
    comparePlans(optimized3, originalAnalyzedQuery3)

    val originalAnalyzedQuery4 = t3.join(t1
      .join(t2, joinType = LeftOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized4 = Optimize.execute(originalAnalyzedQuery4)

    // The plan should remain unchanged due to the left outer join
    comparePlans(optimized4, originalAnalyzedQuery4)

    val originalAnalyzedQuery5 = t3.join(t1
      .join(t2, joinType = RightOuter, condition = Some(id1 === id2)),
      joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized5 = Optimize.execute(originalAnalyzedQuery5)

    // The plan should remain unchanged due to the right outer join
    comparePlans(optimized5, originalAnalyzedQuery5)
  }

  test("The join condition is properly altered in case when the upper join condition " +
    "involves columns from a non-partitioned table") {
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t1, joinType = Inner, condition = Some(id2 === id1)),
      joinType = Inner, condition = Some(id1 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id2 === id3))
      .join(t1, joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id3 === id1)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
      joinType = Inner, condition = Some(id1 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("A proper rotation is applied even in case when a join involves tables " +
    "which do not provide partitioning information") {
    val originalAnalyzedQuery1 = t3.join(t2
      .join(t0, joinType = Inner, condition = Some(id2 === id0)),
      joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t3.join(t2, joinType = Inner, condition = Some(id2 === id3))
      .join(t0, joinType = Inner, condition = Some(id2 === id0)).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t0.join(t2, joinType = Inner, condition = Some(id0 === id2))
      .join(t3, joinType = Inner, condition = Some(id3 === id2)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t0.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
      joinType = Inner, condition = Some(id0 === id2)).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Parent and children unary operators in logical plans are preserved during rotations") {
    // Check the case when there are preceding operators only
    val originalAnalyzedQuery1 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .limit('test).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2)).limit('test).analyze

    comparePlans(optimized1, correctAnswer1)

    // Check the case when there are succeeding operators only
    val originalAnalyzedQuery2 = t2.limit('test)
      .join(t1.limit('test), joinType = Inner, condition = Some(id2 === id1))
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.limit('test).join(t2.limit('test)
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).analyze

    comparePlans(optimized2, correctAnswer2)

    // Check for both preceding and succeeding operators present
    val originalAnalyzedQuery3 = t2.limit('test)
      .join(t1.limit('test), joinType = Inner, condition = Some(id2 === id1))
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3))
      .limit('test).subquery('x).analyze

    val optimized3 = Optimize.execute(originalAnalyzedQuery3)

    val correctAnswer3 = t1.limit('test).join(t2.limit('test)
      .join(t3.limit('test), joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id2 === id1)).limit('test).subquery('x).analyze

    comparePlans(optimized3, correctAnswer3)
  }

  test("Parent and children binary operators in logical plans are preserved during rotations") {
    val originalAnalyzedQuery1 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .intersect(t4.join(t5, joinType = Inner, condition = Some(id4 === id5))
        .join(t6, joinType = Inner, condition = Some(id5 === id6))).limit('test).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2))
      .intersect(t4.join(t5.join(t6, joinType = Inner, condition = Some(id5 === id6)),
        joinType = Inner, condition = Some(id4 === id5))).limit('test).analyze

    comparePlans(optimized1, correctAnswer1)

    val originalAnalyzedQuery2 = t1.join(t2, joinType = Inner, condition = Some(id1 === id2))
      .join(t3, joinType = Inner, condition = Some(id2 === id3))
      .intersect(t4.join(t5, joinType = Inner, condition = Some(id4 === id5))
        .join(t6, joinType = Inner, condition = Some(id5 === id6))).limit('test)
      .unionAll(t0.join(t2, joinType = Inner, condition = Some(id0 === id2))
        .join(t3, joinType = Inner, condition = Some(id3 === id2)).analyze).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t1.join(t2.join(t3, joinType = Inner, condition = Some(id2 === id3)),
      joinType = Inner, condition = Some(id1 === id2))
      .intersect(t4.join(t5.join(t6, joinType = Inner, condition = Some(id5 === id6)),
        joinType = Inner, condition = Some(id4 === id5))).limit('test)
      .unionAll(t0.join(t2.join(t3, joinType = Inner, condition = Some(id3 === id2)),
        joinType = Inner, condition = Some(id0 === id2))).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Attributes' aliases are correctly moved during rotations of logical plans") {
    /**
     * This query corresponds to the following SQL query (please notice the tables' aliases):
     * SELECT COUNT(*) FROM T1 t1
     * JOIN T2 t2 ON (t1.id1 = t2.id2)
     * JOIN T3 t3 ON (t2.id2 = t3.id3)
     * GROUP BY t1.id1
     */
    val originalAnalyzedQuery1 = t1.select(id1)
      .join(t2.select(id2), joinType = Inner, condition = Some(id1 === id2))
      .select(id1, id2).join(t3.select(id3), joinType = Inner, condition = Some(id2 === id3))
      .select(id0).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.select(id1)
      .join(t2.select(id2).join(t3.select(id3), joinType = Inner, condition = Some(id2 === id3))
        .select(id2, id3), joinType = Inner, condition = Some(id1 === id2)).select(id0).analyze

    comparePlans(optimized1, correctAnswer1)

    /**
     * This query corresponds to the following SQL query (please notice the tables' aliases):
     * SELECT COUNT(*) FROM T3 t3
     * JOIN (T2 t2 JOIN T1 t1
     * ON (t2.id2 = t1.id1))
     * ON (t3.id3 = t2.id2)
     * GROUP BY t1.id1
     */
    val originalAnalyzedQuery2 = t3.select(id3)
      .join(t2.select(id2).join(t1.select(id1), joinType = Inner, condition = Some(id2 === id1))
        .select(id2, id1), joinType = Inner, condition = Some(id3 === id2))
      .select(id0).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t3.select(id3)
      .join(t2.select(id2), joinType = Inner, condition = Some(id3 === id2))
      .select(id3, id2).join(t1.select(id1), joinType = Inner, condition = Some(id2 === id1))
      .select(id0).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Attributes' aliases from subqueries are correctly moved during rotations " +
    "of logical plans") {
    val tid2 = id2 as "tid2"
    val tid3 = id3 as "tid3"
    /**
     * This query corresponds to the following SQL query (please notice the fields' aliases):
     * SELECT SUM(id1), SUM(tid2), SUM(tid3) FROM T1 t1
     * JOIN (SELECT id2 AS tid2 FROM T2) AS t2 ON (id1 = tid2)
     * JOIN (SELECT id3 AS tid3 FROM T3) AS t3 ON (tid2 = tid3)
     * GROUP BY id1
     */
    val originalAnalyzedQuery1 = t1.select(id1)
      .join(t2.select(tid2), joinType = Inner, condition = Some(id1 === tid2))
      .select(id1, tid2)
      .join(t3.select(tid3), joinType = Inner, condition = Some(id2 === tid3))
      .select(id1, tid2, tid3).analyze

    val optimized1 = Optimize.execute(originalAnalyzedQuery1)

    val correctAnswer1 = t1.select(id1)
      .join(t2.select(tid2).join(t3.select(tid3),
        joinType = Inner, condition = Some(tid2 === tid3))
        .select(tid2, tid3), joinType = Inner, condition = Some(id1 === tid2))
      .select(id1, tid2, tid3).analyze

    comparePlans(optimized1, correctAnswer1)

    /**
     * This query corresponds to the following SQL query (please notice the table aliases):
     * SELECT SUM(id1), SUM(tid2), SUM(tid3) FROM
     * (SELECT id3 AS tid3 FROM T3) AS t3
     * JOIN ((SELECT id2 AS tid2 FROM T2) AS t2
     * JOIN T1 t1 ON (tid2 = id1)) ON (tid2 = tid2))
     * GROUP BY id1
     */
    val originalAnalyzedQuery2 = t3.select(tid3)
      .join(t2.select(tid2)
        .join(t1.select(id1), joinType = Inner, condition = Some(tid2 === id1))
        .select(tid2, id1), joinType = Inner, condition = Some(tid3 === tid2))
      .select(id1, tid2, tid3).analyze

    val optimized2 = Optimize.execute(originalAnalyzedQuery2)

    val correctAnswer2 = t3.select(tid3)
      .join(t2.select(tid2), joinType = Inner, condition = Some(tid3 === tid2))
      .select(tid3, tid2)
      .join(t1.select(id1), joinType = Inner, condition = Some(tid2 === id1))
      .select(id1, tid2, tid3).analyze

    comparePlans(optimized2, correctAnswer2)
  }

  test("Co-location optimizer rule does not hang for queries" +
    " involving large number of joins (bug 118986)") {
    val ACDOCA_PARAMS = """ RCLNT VARCHAR(3), RLDNR VARCHAR(2), RBUKRS VARCHAR(4),
        |GJAHR VARCHAR(4), BELNR VARCHAR(10), DOCLN VARCHAR(6), RYEAR VARCHAR(4),
        |RRCTY VARCHAR(1), RMVCT VARCHAR(3), VORGN VARCHAR(4), VRGNG VARCHAR(4),
        |BTTYPE VARCHAR(4), AWTYP VARCHAR(5), AWSYS VARCHAR(10), AWORG VARCHAR(10),
        |AWREF VARCHAR(10), AWITEM VARCHAR(6), AWITGRP VARCHAR(6), SUBTA VARCHAR(6),
        |XREVERSING VARCHAR(1), XREVERSED VARCHAR(1), XTRUEREV VARCHAR(1),
        |AWTYP_REV VARCHAR(5), AWORG_REV VARCHAR(10), AWREF_REV VARCHAR(10),
        |SUBTA_REV VARCHAR(6), XSETTLING VARCHAR(1), XSETTLED VARCHAR(1),
        |PREC_AWTYP VARCHAR(5), PREC_AWORG VARCHAR(10), PREC_AWREF VARCHAR(10),
        |PREC_AWITEM VARCHAR(6), PREC_SUBTA VARCHAR(6), RTCUR VARCHAR(5),
        |RWCUR VARCHAR(5), RHCUR VARCHAR(5), RKCUR VARCHAR(5), ROCUR VARCHAR(5),
        |RVCUR VARCHAR(5), RCO_OCUR VARCHAR(5), RUNIT VARCHAR(3), RVUNIT VARCHAR(3),
        |RRUNIT VARCHAR(3), QUNIT1 VARCHAR(3), QUNIT2 VARCHAR(3), QUNIT3 VARCHAR(3),
        |RACCT VARCHAR(10), RCNTR VARCHAR(10), PRCTR VARCHAR(10), RFAREA VARCHAR(16),
        |RBUSA VARCHAR(4), KOKRS VARCHAR(4), SEGMENT VARCHAR(10), SCNTR VARCHAR(10),
        |PPRCTR VARCHAR(10), SFAREA VARCHAR(16), SBUSA VARCHAR(4), RASSC VARCHAR(6),
        |PSEGMENT VARCHAR(10), TSL DECIMAL(18,4), WSL DECIMAL(18,4), HSL DECIMAL(18,4),
        |KSL DECIMAL(18,4), OSL DECIMAL(18,4), VSL DECIMAL(18,4), KFSL DECIMAL(18,4),
        |PSL DECIMAL(18,4), PFSL DECIMAL(18,4), CO_OSL DECIMAL(18,4), HSALK3 DECIMAL(18,4),
        |KSALK3 DECIMAL(18,4), OSALK3 DECIMAL(18,4), VSALK3 DECIMAL(18,4), HSALKV DECIMAL(18,4),
        |KSALKV DECIMAL(18,4), OSALKV DECIMAL(18,4), VSALKV DECIMAL(18,4), HPVPRS DECIMAL(18,4),
        |KPVPRS DECIMAL(18,4), OPVPRS DECIMAL(18,4), VPVPRS DECIMAL(18,4), HSTPRS DECIMAL(18,4),
        |KSTPRS DECIMAL(18,4), OSTPRS DECIMAL(18,4), VSTPRS DECIMAL(18,4), HSLALT DECIMAL(18,4),
        |KSLALT DECIMAL(18,4), OSLALT DECIMAL(18,4), VSLALT DECIMAL(18,4), HSLEXT DECIMAL(18,4),
        |KSLEXT DECIMAL(18,4), OSLEXT DECIMAL(18,4), VSLEXT DECIMAL(18,4), HVKWRT DECIMAL(18,4),
        |HVKSAL DECIMAL(18,4), MSL DECIMAL(18,4), MFSL DECIMAL(18,4), VMSL DECIMAL(18,4),
        |VMFSL DECIMAL(18,4), RMSL DECIMAL(18,4), QUANT1 DECIMAL(18,4), QUANT2 DECIMAL(18,4),
        |QUANT3 DECIMAL(18,4), LBKUM DECIMAL(18,4), DRCRK VARCHAR(1), POPER VARCHAR(3),
        |PERIV VARCHAR(2), FISCYEARPER VARCHAR(7), BUDAT VARCHAR(8), BLDAT VARCHAR(8),
        |BLART VARCHAR(2), BUZEI VARCHAR(3), ZUONR VARCHAR(18), BSCHL VARCHAR(2),
        |BSTAT VARCHAR(1), LINETYPE VARCHAR(5), KTOSL VARCHAR(3), SLALITTYPE VARCHAR(5),
        |XSPLITMOD VARCHAR(1), USNAM VARCHAR(12), TIMESTAMP_2 DECIMAL(18,4), EPRCTR VARCHAR(10),
        |RHOART VARCHAR(2), GLACCOUNT_TYPE VARCHAR(1), KTOPL VARCHAR(4), LOKKT VARCHAR(10),
        |KTOP2 VARCHAR(4), REBZG VARCHAR(10), REBZJ VARCHAR(4), REBZZ VARCHAR(3), REBZT VARCHAR(1),
        |RBEST VARCHAR(3), EBELN VARCHAR(10), EBELP VARCHAR(5), ZEKKN VARCHAR(2),
        |SGTXT VARCHAR(50), KDAUF VARCHAR(10), KDPOS VARCHAR(6), MATNR VARCHAR(18),
        |WERKS VARCHAR(4), LIFNR VARCHAR(10), KUNNR VARCHAR(10), KOART VARCHAR(1),
        |UMSKZ VARCHAR(1), MWSKZ VARCHAR(2), HBKID VARCHAR(5), HKTID VARCHAR(5), XOPVW VARCHAR(1),
        |AUGDT VARCHAR(8), AUGBL VARCHAR(10), AUGGJ VARCHAR(4), AFABE VARCHAR(2),
        |ANLN1 VARCHAR(12), ANLN2 VARCHAR(4), BZDAT VARCHAR(8), ANBWA VARCHAR(3),
        |MOVCAT VARCHAR(2), DEPR_PERIOD VARCHAR(3), ANLGR VARCHAR(12), ANLGR2 VARCHAR(4),
        |SETTLEMENT_RULE VARCHAR(3), KALNR VARCHAR(12), VPRSV VARCHAR(1), MLAST VARCHAR(1),
        |KZBWS VARCHAR(1), XOBEW VARCHAR(1), SOBKZ VARCHAR(1), VTSTAMP DECIMAL(18,4),
        |MAT_KDAUF VARCHAR(10), MAT_KDPOS VARCHAR(6), MAT_PSPNR VARCHAR(8),
        |MAT_PS_POSID VARCHAR(24), MAT_LIFNR VARCHAR(10), BWTAR VARCHAR(10),
        |BWKEY VARCHAR(4), HPEINH DECIMAL(18,4), KPEINH DECIMAL(18,4),
        |OPEINH DECIMAL(18,4), VPEINH DECIMAL(18,4), MLPTYP VARCHAR(4),
        |MLCATEG VARCHAR(2), QSBVALT VARCHAR(12), QSPROCESS VARCHAR(12), PERART VARCHAR(2),
        |RACCT_SENDER VARCHAR(10), ACCAS_SENDER VARCHAR(30), ACCASTY_SENDER VARCHAR(2),
        |OBJNR VARCHAR(22), HRKFT VARCHAR(14), HKGRP VARCHAR(4), PAROB1 VARCHAR(22),
        |PAROBSRC VARCHAR(1), USPOB VARCHAR(22), CO_BELKZ VARCHAR(1), CO_BEKNZ VARCHAR(1),
        |BELTP VARCHAR(1), MUVFLG VARCHAR(1), GKONT VARCHAR(10), GKOAR VARCHAR(1),
        |ERLKZ VARCHAR(1), PERNR VARCHAR(8), PAOBJNR VARCHAR(10), XPAOBJNR_CO_REL VARCHAR(1),
        |SCOPE VARCHAR(2), LOGSYSO VARCHAR(10), PBUKRS VARCHAR(4), PSCOPE VARCHAR(2),
        |LOGSYSP VARCHAR(10), BWSTRAT VARCHAR(1), OBJNR_HK VARCHAR(22), AUFNR_ORG VARCHAR(12),
        |UKOSTL VARCHAR(10), ULSTAR VARCHAR(6), UPRZNR VARCHAR(12), ACCAS VARCHAR(30),
        |ACCASTY VARCHAR(2), LSTAR VARCHAR(6), AUFNR VARCHAR(12), AUTYP VARCHAR(2),
        |PS_POSID VARCHAR(24), PS_PSPID VARCHAR(24), NPLNR VARCHAR(12), NPLNR_VORGN VARCHAR(4),
        |PRZNR VARCHAR(12), KSTRG VARCHAR(12), BEMOT VARCHAR(2), QMNUM VARCHAR(12),
        |ERKRS VARCHAR(4), PACCAS VARCHAR(30), PACCASTY VARCHAR(2), PLSTAR VARCHAR(6),
        |PAUFNR VARCHAR(12), PAUTYP VARCHAR(2), PPS_POSID VARCHAR(24), PPS_PSPID VARCHAR(24),
        |PKDAUF VARCHAR(10), PKDPOS VARCHAR(6), PPAOBJNR VARCHAR(10), PNPLNR VARCHAR(12),
        |PNPLNR_VORGN VARCHAR(4), PPRZNR VARCHAR(12), PKSTRG VARCHAR(12), CO_ACCASTY_N1 VARCHAR(2),
        |CO_ACCASTY_N2 VARCHAR(2), CO_ACCASTY_N3 VARCHAR(2), CO_ZLENR VARCHAR(3),
        |CO_BELNR VARCHAR(10), CO_BUZEI VARCHAR(3), CO_REFBZ VARCHAR(3), FKART VARCHAR(4),
        |VKORG VARCHAR(4), VTWEG VARCHAR(2), SPART VARCHAR(2), MATNR_COPA VARCHAR(18),
        |MATKL VARCHAR(9), KDGRP VARCHAR(2), ACDOC_COPA_EEW_DUMMY_PA VARCHAR(1),
        |ABTNR_PA VARCHAR(4), EKORG_PA VARCHAR(4), WGRU1_PA VARCHAR(18), WGRU2_PA VARCHAR(18),
        |KMMAKL_PA VARCHAR(9), BZIRK_PA VARCHAR(6), VKGRP_PA VARCHAR(3), BRSCH_PA VARCHAR(4),
        |LAND1_PA VARCHAR(3), MAABC_PA VARCHAR(1), BONUS_PA VARCHAR(2), VKBUR_PA VARCHAR(4),
        |EFORM_PA VARCHAR(5), GEBIE_PA VARCHAR(4), HIE01_PA VARCHAR(10), HIE02_PA VARCHAR(10),
        |HIE03_PA VARCHAR(10), HIE04_PA VARCHAR(10), HIE05_PA VARCHAR(10), HIE06_PA VARCHAR(10),
        |HIE07_PA VARCHAR(10), KTGRD_PA VARCHAR(2), KUNWE_PA VARCHAR(10), PAPH1_PA VARCHAR(5),
        |PAPH2_PA VARCHAR(10), PAPH3_PA VARCHAR(18), VRTNR_PA VARCHAR(8), WWGL_PA VARCHAR(8),
        |WWPG1_PA VARCHAR(5), WWPG2_PA VARCHAR(10), WWPG3_PA VARCHAR(18), WWPRC_PA VARCHAR(8),
        |WWVL_PA VARCHAR(8), REGIO_PA VARCHAR(3), SATNR_PA VARCHAR(18), RE_BUKRS VARCHAR(4),
        |RE_ACCOUNT VARCHAR(10), FIKRS VARCHAR(4), FISTL VARCHAR(16), MEASURE VARCHAR(24),
        |RFUND VARCHAR(10), RGRANT_NBR VARCHAR(20), RBUDGET_PD VARCHAR(10), SFUND VARCHAR(10),
        |SGRANT_NBR VARCHAR(20), SBUDGET_PD VARCHAR(10), VNAME VARCHAR(6), EGRUP VARCHAR(3),
        |RECID VARCHAR(2), VPTNR VARCHAR(10), BTYPE VARCHAR(2), ETYPE VARCHAR(3),
        |PRODPER VARCHAR(8), SWENR VARCHAR(8), SGENR VARCHAR(8), SGRNR VARCHAR(8),
        |SMENR VARCHAR(8), RECNNR VARCHAR(13), SNKSL VARCHAR(4), SEMPSL VARCHAR(5),
        |DABRZ VARCHAR(8), PSWENR VARCHAR(8), PSGENR VARCHAR(8), PSGRNR VARCHAR(8),
        |PSMENR VARCHAR(8), PRECNNR VARCHAR(13), PSNKSL VARCHAR(4), PSEMPSL VARCHAR(5),
        |PDABRZ VARCHAR(8), ACDOC_EEW_DUMMY VARCHAR(1), ZZSPREG VARCHAR(5), ZZBUSPARTN VARCHAR(10),
        |ZZPRODUCT VARCHAR(10), ZZLOCA VARCHAR(4), ZZCHAN VARCHAR(4), ZZLOB VARCHAR(7),
        |MIG_SOURCE VARCHAR(1), MIG_DOCLN VARCHAR(6), _DATAAGING VARCHAR(8)""".stripMargin
    val FINSC_LEDGER_REP_PARAMS =
      """MANDT VARCHAR(3), RLDNR VARCHAR(2), RLDNR_PERS VARCHAR(2)""".stripMargin
    val FINSC_LEDGER_T_PARAMS =
      """MANDT VARCHAR(3), LANGU VARCHAR(1), RLDNR VARCHAR(2), NAME VARCHAR(30)""".stripMargin
    val FINSC_LD_CMP_PARAMS = """MANDT VARCHAR(3), RLDNR VARCHAR(2), BUKRS VARCHAR(4),
        |CURTPH VARCHAR(2), RATETYPEH VARCHAR(4), CURSRH VARCHAR(1), CURDTH VARCHAR(1),
        |MLRELINDH VARCHAR(1), CURTPK VARCHAR(2), RATETYPEK VARCHAR(4), CURSRK VARCHAR(1),
        |CURDTK VARCHAR(1), CURPOSK VARCHAR(1), MLRELINDK VARCHAR(1), CURTPO VARCHAR(2),
        |RATETYPEO VARCHAR(4), CURSRO VARCHAR(1), CURDTO VARCHAR(1), CURPOSO VARCHAR(1),
        |MLRELINDO VARCHAR(1), CURTPV VARCHAR(2), RATETYPEV VARCHAR(4), CURSRV VARCHAR(1),
        |CURDTV VARCHAR(1), CURPOSV VARCHAR(1), MLRELINDV VARCHAR(1), PERIV VARCHAR(2),
        |PARGLACCTS VARCHAR(1), VERSN VARCHAR(3), OPVAR VARCHAR(4), TOYEAR VARCHAR(4)
      """.stripMargin
    val T001_PARAMS =
      """MANDT VARCHAR(3), BUKRS VARCHAR(4), BUTXT VARCHAR(25), ORT01 VARCHAR(25),
        |LAND1 VARCHAR(3), WAERS VARCHAR(5), SPRAS VARCHAR(1), KTOPL VARCHAR(4),
        |WAABW VARCHAR(2), PERIV VARCHAR(2), KOKFI VARCHAR(1), RCOMP VARCHAR(6),
        |ADRNR VARCHAR(10), STCEG VARCHAR(20), FIKRS VARCHAR(4), XFMCO VARCHAR(1),
        |XFMCB VARCHAR(1), XFMCA VARCHAR(1), TXJCD VARCHAR(15), FMHRDATE VARCHAR(8),
        |BUVAR VARCHAR(1), FDBUK VARCHAR(4), XFDIS VARCHAR(1), XVALV VARCHAR(1),
        |XSKFN VARCHAR(1), KKBER VARCHAR(4), XMWSN VARCHAR(1), MREGL VARCHAR(4),
        |XGSBE VARCHAR(1), XGJRV VARCHAR(1), XKDFT VARCHAR(1), XPROD VARCHAR(1),
        |XEINK VARCHAR(1), XJVAA VARCHAR(1), XVVWA VARCHAR(1), XSLTA VARCHAR(1),
        |XFDMM VARCHAR(1), XFDSD VARCHAR(1), XEXTB VARCHAR(1), EBUKR VARCHAR(4),
        |KTOP2 VARCHAR(4), UMKRS VARCHAR(4), BUKRS_GLOB VARCHAR(6), FSTVA VARCHAR(4),
        |OPVAR VARCHAR(4), XCOVR VARCHAR(1), TXKRS VARCHAR(1), WFVAR VARCHAR(4),
        |XBBBF VARCHAR(1), XBBBE VARCHAR(1), XBBBA VARCHAR(1), XBBKO VARCHAR(1),
        |XSTDT VARCHAR(1), MWSKV VARCHAR(2), MWSKA VARCHAR(2), IMPDA VARCHAR(1),
        |XNEGP VARCHAR(1), XKKBI VARCHAR(1), WT_NEWWT VARCHAR(1), PP_PDATE VARCHAR(1),
        |INFMT VARCHAR(4), FSTVARE VARCHAR(4), KOPIM VARCHAR(1), DKWEG VARCHAR(1),
        |OFFSACCT VARCHAR(1), BAPOVAR VARCHAR(2), XCOS VARCHAR(1), XCESSION VARCHAR(1),
        |XSPLT VARCHAR(1), SURCCM VARCHAR(1), DTPROV VARCHAR(2), DTAMTC VARCHAR(2),
        |DTTAXC VARCHAR(2), DTTDSP VARCHAR(2), DTAXR VARCHAR(4), XVATDATE VARCHAR(1),
        |PST_PER_VAR VARCHAR(1), XBBSC VARCHAR(1), FM_DERIVE_ACC VARCHAR(1),
        |XTEMPLT VARCHAR(1)""".stripMargin
    val TKA01_PARAMS = """MANDT VARCHAR(3), KOKRS VARCHAR(4), BEZEI VARCHAR(25), WAERS VARCHAR(5),
        |KTOPL VARCHAR(4), LMONA VARCHAR(2), KOKFI VARCHAR(1), LOGSYSTEM VARCHAR(10),
        |ALEMT VARCHAR(2), MD_LOGSYSTEM VARCHAR(10), KHINR VARCHAR(12), KOMP1 VARCHAR(1),
        |KOMP0 VARCHAR(1), KOMP2 VARCHAR(1), ERKRS VARCHAR(4), DPRCT VARCHAR(10),
        |PHINR VARCHAR(12), PCLDG VARCHAR(2), PCBEL VARCHAR(1), XWBUK VARCHAR(1),
        |BPHINR VARCHAR(12), XBPALE VARCHAR(1), KSTAR_FIN VARCHAR(10), KSTAR_FID VARCHAR(10),
        |PCACUR VARCHAR(5), PCACURTP VARCHAR(2), PCATRCUR VARCHAR(1), CTYP VARCHAR(2),
        |RCLAC VARCHAR(1), BLART VARCHAR(2), FIKRS VARCHAR(4), RCL_PRIMAC VARCHAR(1),
        |PCA_ALEMT VARCHAR(2), PCA_VALU VARCHAR(1), CVPROF VARCHAR(4), CVACT VARCHAR(1),
        |VNAME VARCHAR(12), PCA_ACC_DIFF VARCHAR(1), TP_VALOHB VARCHAR(1), DEFPRCTR VARCHAR(10),
        |AUTH_USE_NO_STD VARCHAR(1), AUTH_USE_ADD1 VARCHAR(1), AUTH_USE_ADD2 VARCHAR(1),
        |AUTH_KE_NO_STD VARCHAR(1), AUTH_KE_USE_ADD1 VARCHAR(1), AUTH_KE_USE_ADD2 VARCHAR(1)
      """.stripMargin
    val KNA1_PARAMS = """MANDT VARCHAR(3), KUNNR VARCHAR(10), LAND1 VARCHAR(3), NAME1 VARCHAR(35),
        |NAME2 VARCHAR(35), ORT01 VARCHAR(35), PSTLZ VARCHAR(10), REGIO VARCHAR(3),
        |SORTL VARCHAR(10), STRAS VARCHAR(35), TELF1 VARCHAR(16), TELFX VARCHAR(31),
        |XCPDK VARCHAR(1), ADRNR VARCHAR(10), MCOD1 VARCHAR(25), MCOD2 VARCHAR(25),
        |MCOD3 VARCHAR(25), ANRED VARCHAR(15), AUFSD VARCHAR(2), BAHNE VARCHAR(25),
        |BAHNS VARCHAR(25), BBBNR VARCHAR(7), BBSNR VARCHAR(5), BEGRU VARCHAR(4),
        |BRSCH VARCHAR(4), BUBKZ VARCHAR(1), DATLT VARCHAR(14), ERDAT VARCHAR(8),
        |ERNAM VARCHAR(12), EXABL VARCHAR(1), FAKSD VARCHAR(2), FISKN VARCHAR(10),
        |KNAZK VARCHAR(2), KNRZA VARCHAR(10), KONZS VARCHAR(10), KTOKD VARCHAR(4),
        |KUKLA VARCHAR(2), LIFNR VARCHAR(10), LIFSD VARCHAR(2), LOCCO VARCHAR(10),
        |LOEVM VARCHAR(1), NAME3 VARCHAR(35), NAME4 VARCHAR(35), NIELS VARCHAR(2),
        |ORT02 VARCHAR(35), PFACH VARCHAR(10), PSTL2 VARCHAR(10), COUNC VARCHAR(3),
        |CITYC VARCHAR(4), RPMKR VARCHAR(5), SPERR VARCHAR(1), SPRAS VARCHAR(1),
        |STCD1 VARCHAR(16), STCD2 VARCHAR(11), STKZA VARCHAR(1), STKZU VARCHAR(1),
        |TELBX VARCHAR(15), TELF2 VARCHAR(16), TELTX VARCHAR(30), TELX1 VARCHAR(30),
        |LZONE VARCHAR(10), XZEMP VARCHAR(1), VBUND VARCHAR(6), STCEG VARCHAR(20),
        |DEAR1 VARCHAR(1), DEAR2 VARCHAR(1), DEAR3 VARCHAR(1), DEAR4 VARCHAR(1),
        |DEAR5 VARCHAR(1), GFORM VARCHAR(2), BRAN1 VARCHAR(10), BRAN2 VARCHAR(10),
        |BRAN3 VARCHAR(10), BRAN4 VARCHAR(10), BRAN5 VARCHAR(10), EKONT VARCHAR(10),
        |UMSAT DECIMAL(8,2), UMJAH VARCHAR(4), UWAER VARCHAR(5), JMZAH VARCHAR(6),
        |JMJAH VARCHAR(4), KATR1 VARCHAR(2), KATR2 VARCHAR(2), KATR3 VARCHAR(2),
        |KATR4 VARCHAR(2), KATR5 VARCHAR(2), KATR6 VARCHAR(3), KATR7 VARCHAR(3),
        |KATR8 VARCHAR(3), KATR9 VARCHAR(3), KATR10 VARCHAR(3), STKZN VARCHAR(1),
        |UMSA1 DECIMAL(15,2), TXJCD VARCHAR(15), PERIV VARCHAR(2), ABRVW VARCHAR(3),
        |INSPBYDEBI VARCHAR(1), INSPATDEBI VARCHAR(1), KTOCD VARCHAR(4),
        |PFORT VARCHAR(35), WERKS VARCHAR(4), DTAMS VARCHAR(1), DTAWS VARCHAR(2),
        |DUEFL VARCHAR(1), HZUOR VARCHAR(2), SPERZ VARCHAR(1), ETIKG VARCHAR(10),
        |CIVVE VARCHAR(1), MILVE VARCHAR(1), KDKG1 VARCHAR(2), KDKG2 VARCHAR(2),
        |KDKG3 VARCHAR(2), KDKG4 VARCHAR(2), KDKG5 VARCHAR(2), XKNZA VARCHAR(1),
        |FITYP VARCHAR(2), STCDT VARCHAR(2), STCD3 VARCHAR(18), STCD4 VARCHAR(18),
        |STCD5 VARCHAR(60), XICMS VARCHAR(1), XXIPI VARCHAR(1), XSUBT VARCHAR(3),
        |CFOPC VARCHAR(2), TXLW1 VARCHAR(3), TXLW2 VARCHAR(3), CCC01 VARCHAR(1),
        |CCC02 VARCHAR(1), CCC03 VARCHAR(1), CCC04 VARCHAR(1), CASSD VARCHAR(2),
        |KNURL VARCHAR(132), J_1KFREPRE VARCHAR(10), J_1KFTBUS VARCHAR(30),
        |J_1KFTIND VARCHAR(30), CONFS VARCHAR(1), UPDAT VARCHAR(8), UPTIM VARCHAR(6),
        |NODEL VARCHAR(1), DEAR6 VARCHAR(1), SUFRAMA VARCHAR(9), RG VARCHAR(11),
        |EXP VARCHAR(3), UF VARCHAR(2), RGDATE VARCHAR(8), RIC VARCHAR(11),
        |RNE VARCHAR(10), RNEDATE VARCHAR(8), CNAE VARCHAR(7), LEGALNAT VARCHAR(4),
        |CRTN VARCHAR(1), ICMSTAXPAY VARCHAR(2), INDTYP VARCHAR(2), TDT VARCHAR(2),
        |COMSIZE VARCHAR(2), DECREGPC VARCHAR(2), _VSO_R_PALHGT DECIMAL(13,3),
        |_VSO_R_PAL_UL VARCHAR(3), _VSO_R_PK_MAT VARCHAR(1), _VSO_R_MATPAL VARCHAR(18),
        |_VSO_R_I_NO_LYR VARCHAR(2), _VSO_R_ONE_MAT VARCHAR(1), _VSO_R_ONE_SORT VARCHAR(1),
        |_VSO_R_ULD_SIDE VARCHAR(1), _VSO_R_LOAD_PREF VARCHAR(1), _VSO_R_DPOINT VARCHAR(10),
        |ALC VARCHAR(8), PMT_OFFICE VARCHAR(5), FEE_SCHEDULE VARCHAR(4), DUNS VARCHAR(9),
        |DUNS4 VARCHAR(4), PSOFG VARCHAR(10), PSOIS VARCHAR(20), PSON1 VARCHAR(35),
        |PSON2 VARCHAR(35), PSON3 VARCHAR(35), PSOVN VARCHAR(35), PSOTL VARCHAR(20),
        |PSOHS VARCHAR(6), PSOST VARCHAR(28), PSOO1 VARCHAR(50), PSOO2 VARCHAR(50),
        |PSOO3 VARCHAR(50), PSOO4 VARCHAR(50), PSOO5 VARCHAR(50), CVP_XBLCK VARCHAR(1)
      """.stripMargin
    val LFA1_PARAMS = """MANDT VARCHAR(3), LIFNR VARCHAR(10), LAND1 VARCHAR(3),
        |NAME1 VARCHAR(35), NAME2 VARCHAR(35), NAME3 VARCHAR(35), NAME4 VARCHAR(35),
        |ORT01 VARCHAR(35), ORT02 VARCHAR(35), PFACH VARCHAR(10), PSTL2 VARCHAR(10),
        |PSTLZ VARCHAR(10), REGIO VARCHAR(3), SORTL VARCHAR(10), STRAS VARCHAR(35),
        |ADRNR VARCHAR(10), MCOD1 VARCHAR(25), MCOD2 VARCHAR(25), MCOD3 VARCHAR(25),
        |ANRED VARCHAR(15), BAHNS VARCHAR(25), BBBNR VARCHAR(7), BBSNR VARCHAR(5),
        |BEGRU VARCHAR(4), BRSCH VARCHAR(4), BUBKZ VARCHAR(1), DATLT VARCHAR(14),
        |DTAMS VARCHAR(1), DTAWS VARCHAR(2), ERDAT VARCHAR(8), ERNAM VARCHAR(12),
        |ESRNR VARCHAR(11), KONZS VARCHAR(10), KTOKK VARCHAR(4), KUNNR VARCHAR(10),
        |LNRZA VARCHAR(10), LOEVM VARCHAR(1), SPERR VARCHAR(1), SPERM VARCHAR(1),
        |SPRAS VARCHAR(1), STCD1 VARCHAR(16), STCD2 VARCHAR(11), STKZA VARCHAR(1),
        |STKZU VARCHAR(1), TELBX VARCHAR(15), TELF1 VARCHAR(16), TELF2 VARCHAR(16),
        |TELFX VARCHAR(31), TELTX VARCHAR(30), TELX1 VARCHAR(30), XCPDK VARCHAR(1),
        |XZEMP VARCHAR(1), VBUND VARCHAR(6), FISKN VARCHAR(10), STCEG VARCHAR(20),
        |STKZN VARCHAR(1), SPERQ VARCHAR(2), GBORT VARCHAR(25), GBDAT VARCHAR(8),
        |SEXKZ VARCHAR(1), KRAUS VARCHAR(11), REVDB VARCHAR(8), QSSYS VARCHAR(4),
        |KTOCK VARCHAR(4), PFORT VARCHAR(35), WERKS VARCHAR(4), LTSNA VARCHAR(1),
        |WERKR VARCHAR(1), PLKAL VARCHAR(2), DUEFL VARCHAR(1), TXJCD VARCHAR(15),
        |SPERZ VARCHAR(1), SCACD VARCHAR(4), SFRGR VARCHAR(4), LZONE VARCHAR(10),
        |XLFZA VARCHAR(1), DLGRP VARCHAR(4), FITYP VARCHAR(2), STCDT VARCHAR(2),
        |REGSS VARCHAR(1), ACTSS VARCHAR(3), STCD3 VARCHAR(18), STCD4 VARCHAR(18),
        |STCD5 VARCHAR(60), IPISP VARCHAR(1), TAXBS VARCHAR(1), PROFS VARCHAR(30),
        |STGDL VARCHAR(2), EMNFR VARCHAR(10), LFURL VARCHAR(132), J_1KFREPRE VARCHAR(10),
        |J_1KFTBUS VARCHAR(30), J_1KFTIND VARCHAR(30), CONFS VARCHAR(1), UPDAT VARCHAR(8),
        |UPTIM VARCHAR(6), NODEL VARCHAR(1), QSSYSDAT VARCHAR(8), PODKZB VARCHAR(1),
        |FISKU VARCHAR(10), STENR VARCHAR(18), CARRIER_CONF VARCHAR(1), MIN_COMP VARCHAR(1),
        |TERM_LI VARCHAR(1), CRC_NUM VARCHAR(25), RG VARCHAR(11), EXP VARCHAR(3),
        |UF VARCHAR(2), RGDATE VARCHAR(8), RIC VARCHAR(11), RNE VARCHAR(10),
        |RNEDATE VARCHAR(8), CNAE VARCHAR(7), LEGALNAT VARCHAR(4), CRTN VARCHAR(1),
        |ICMSTAXPAY VARCHAR(2), INDTYP VARCHAR(2), TDT VARCHAR(2), COMSIZE VARCHAR(2),
        |DECREGPC VARCHAR(2), J_SC_CAPITAL DECIMAL(15,2), J_SC_CURRENCY VARCHAR(5),
        |ALC VARCHAR(8), PMT_OFFICE VARCHAR(5), PPA_RELEVANT VARCHAR(1),
        |PSOFG VARCHAR(10), PSOIS VARCHAR(20), PSON1 VARCHAR(35), PSON2 VARCHAR(35),
        |PSON3 VARCHAR(35), PSOVN VARCHAR(35), PSOTL VARCHAR(20), PSOHS VARCHAR(6),
        |PSOST VARCHAR(28), TRANSPORT_CHAIN VARCHAR(10), STAGING_TIME DECIMAL(3,0),
        |SCHEDULING_TYPE VARCHAR(1), SUBMI_RELEVANT VARCHAR(1),
        |CVP_XBLCK VARCHAR(1)""".stripMargin
    val SKAT_PARAMS =
      """MANDT VARCHAR(3), SPRAS VARCHAR(1), KTOPL VARCHAR(4), SAKNR VARCHAR(10),
        |TXT20 VARCHAR(20), TXT50 VARCHAR(50), MCOD1 VARCHAR(25)""".stripMargin
    val FAGL_SEGMT_PARAMS =
      """MANDT VARCHAR(3), LANGU VARCHAR(1), SEGMENT VARCHAR(10), NAME VARCHAR(50)
      """.stripMargin
    val TFKBT_PARAMS =
      """MANDT VARCHAR(3), SPRAS VARCHAR(1), FKBER VARCHAR(16), FKBTX VARCHAR(25)
      """.stripMargin
    val TGSBT_PARAMS =
      """MANDT VARCHAR(3), SPRAS VARCHAR(1), GSBER VARCHAR(4), GTEXT VARCHAR(30)
      """.stripMargin
    val T003T_PARAMS =
      """MANDT VARCHAR(3), SPRAS VARCHAR(1), BLART VARCHAR(2), LTEXT VARCHAR(20)
      """.stripMargin
    val CEPC_PARAMS =
      """MANDT VARCHAR(3), PRCTR VARCHAR(10), DATBI VARCHAR(8), KOKRS VARCHAR(4),
        |DATAB VARCHAR(8), ERSDA VARCHAR(8), USNAM VARCHAR(12), MERKMAL VARCHAR(30),
        |ABTEI VARCHAR(12), VERAK VARCHAR(20), VERAK_USER VARCHAR(12), WAERS VARCHAR(5),
        |NPRCTR VARCHAR(10), LAND1 VARCHAR(3), ANRED VARCHAR(15), NAME1 VARCHAR(35),
        |NAME2 VARCHAR(35), NAME3 VARCHAR(35), NAME4 VARCHAR(35), ORT01 VARCHAR(35),
        |ORT02 VARCHAR(35), STRAS VARCHAR(35), PFACH VARCHAR(10), PSTLZ VARCHAR(10),
        |PSTL2 VARCHAR(10), SPRAS VARCHAR(1), TELBX VARCHAR(15), TELF1 VARCHAR(16),
        |TELF2 VARCHAR(16), TELFX VARCHAR(31), TELTX VARCHAR(30), TELX1 VARCHAR(30),
        |DATLT VARCHAR(14), DRNAM VARCHAR(4), KHINR VARCHAR(12), BUKRS VARCHAR(4),
        |VNAME VARCHAR(6), RECID VARCHAR(2), ETYPE VARCHAR(3), TXJCD VARCHAR(15),
        |REGIO VARCHAR(3), KVEWE VARCHAR(1), KAPPL VARCHAR(2), KALSM VARCHAR(6),
        |LOGSYSTEM VARCHAR(10), LOCK_IND VARCHAR(1), PCA_TEMPLATE VARCHAR(10),
        |SEGMENT VARCHAR(10)""".stripMargin
    val CEPCT_PARAMS =
      """MANDT VARCHAR(3), SPRAS VARCHAR(1), PRCTR VARCHAR(10), DATBI VARCHAR(8),
        |KOKRS VARCHAR(4), KTEXT VARCHAR(20), LTEXT VARCHAR(40), MCTXT VARCHAR(20)
      """.stripMargin

    sqlContext.sql(s"CREATE TABLE ACDOCA ($ACDOCA_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(
      s"""CREATE TABLE FINSC_LEDGER_REP ($FINSC_LEDGER_REP_PARAMS) USING com.sap.spark.dstest
       """.stripMargin)
    sqlContext.sql(
      s"""CREATE TABLE FINSC_LEDGER_T ($FINSC_LEDGER_T_PARAMS) USING com.sap.spark.dstest
       """.stripMargin)
    sqlContext.sql(s"CREATE TABLE FINSC_LD_CMP ($FINSC_LD_CMP_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE T001 ($T001_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE TKA01 ($TKA01_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE KNA1 ($KNA1_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE LFA1 ($LFA1_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE SKAT ($SKAT_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE FAGL_SEGMT ($FAGL_SEGMT_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE TFKBT ($TFKBT_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE TGSBT ($TGSBT_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE T003T ($T003T_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE CEPC ($CEPC_PARAMS) USING com.sap.spark.dstest")
    sqlContext.sql(s"CREATE TABLE CEPCT ($CEPCT_PARAMS) USING com.sap.spark.dstest")

    val VIEW_FROM =
      """
        | ACDOCA
        | INNER JOIN FINSC_LEDGER_REP ON
        | ( ACDOCA.RCLNT = FINSC_LEDGER_REP.MANDT AND
        | FINSC_LEDGER_REP.RLDNR_PERS = ACDOCA.RLDNR )
        |  INNER JOIN T001 ON
        | ( ACDOCA.RCLNT = T001.MANDT AND T001.BUKRS = ACDOCA.RBUKRS )
        |   LEFT OUTER JOIN SKAT ON
        |  ( SKAT.KTOPL = ACDOCA.KTOPL AND SKAT.SAKNR = ACDOCA.RACCT AND
        |  SKAT.SPRAS = "E" AND ACDOCA.RCLNT = SKAT.MANDT )
        |   LEFT OUTER JOIN TKA01 ON
        | ( ACDOCA.RCLNT = TKA01.MANDT AND TKA01.KOKRS = ACDOCA.KOKRS )
        |   LEFT OUTER JOIN T003T ON
        |  ( T003T.BLART = ACDOCA.BLART AND T003T.SPRAS = "E" AND ACDOCA.RCLNT = T003T.MANDT )
        |      LEFT OUTER JOIN FINSC_LEDGER_T ON
        | ( FINSC_LEDGER_T.RLDNR = ACDOCA.RLDNR AND FINSC_LEDGER_T.LANGU = "E"
        | AND ACDOCA.RCLNT = FINSC_LEDGER_T.MANDT )
        |    LEFT OUTER JOIN FAGL_SEGMT ON
        |  ( FAGL_SEGMT.SEGMENT = ACDOCA.SEGMENT AND FAGL_SEGMT.LANGU = "E"
        |  AND ACDOCA.RCLNT = FAGL_SEGMT.MANDT )
        |  LEFT OUTER JOIN TFKBT ON
        |  ( TFKBT.FKBER = ACDOCA.RFAREA AND TFKBT.SPRAS = "E" AND ACDOCA.RCLNT = TFKBT.MANDT )
        | LEFT OUTER JOIN TGSBT ON
        | ( TGSBT.GSBER = ACDOCA.RBUSA AND TGSBT.SPRAS = "E" AND ACDOCA.RCLNT = TGSBT.MANDT )
        |    LEFT OUTER JOIN CEPC ON
        |  ( CEPC.KOKRS = ACDOCA.KOKRS AND CEPC.PRCTR = ACDOCA.PRCTR AND CEPC.DATAB <= 20160201
        |  AND CEPC.DATBI >= 20160201 AND ACDOCA.RCLNT = CEPC.MANDT )
        |   LEFT OUTER JOIN CEPCT CEPCT ON
        |   ( CEPCT.KOKRS = CEPC.KOKRS AND CEPCT.PRCTR = CEPC.PRCTR
        |   AND CEPCT.DATBI = CEPC.DATBI AND CEPCT.SPRAS = "E" AND
        |   ACDOCA.RCLNT = CEPCT.MANDT )
      """.stripMargin
    val VIEW_COLUMNS =
      """
        | ACDOCA.RCLNT AS MANDT,
        | FINSC_LEDGER_REP.RLDNR,
        | FINSC_LEDGER_REP.RLDNR_PERS,
        | T001.BUTXT AS RBUKRS_NAME,
        | SKAT.TXT20 AS RACCT_TXT20,
        | SKAT.TXT50 AS RACCT_TXT50,
        | CEPCT.KTEXT AS PRCTR_NAME,
        | ACDOCA.RFAREA,
        | TFKBT.FKBTX AS RFAREA_NAME,
        | ACDOCA.RBUSA,
        | TGSBT.GTEXT AS RBUSA_NAME,
        | ACDOCA.KOKRS,
        | TKA01.BEZEI AS KOKRS_NAME,
        | FAGL_SEGMT.NAME AS SEGMENT_NAME,
        |  T003T.LTEXT AS BLART_NAME
      """.stripMargin

    sqlContext.sql(
      s"""CREATE VIEW ZJKV_GLLIT_04 AS
         |SELECT $VIEW_COLUMNS
         |FROM $VIEW_FROM
         |USING com.sap.spark.dstest
         """.stripMargin)

    // This is expected - plans for the relations are not registered
    failAfter(1 minute) {
      intercept[Error] {
        sqlContext.sql("SELECT COUNT(*) FROM ZJKV_GLLIT_04").collect()
      }
    }
  }

}
