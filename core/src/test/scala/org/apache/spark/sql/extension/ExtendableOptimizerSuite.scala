package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, FiltersReduction, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.scalatest.{FunSuite, PrivateMethodTester}

class ExtendableOptimizerSuite extends FunSuite with PrivateMethodTester {

  implicit class OptimizerOps(opt: RuleExecutor[LogicalPlan]) {
    private val nameMethod = PrivateMethod[String]('name)
    private def batches: Seq[AnyRef] = {
      /* XXX: PrivateMethod throws exception "IllegalArgumentException: Found two methods" */
      val clazz = opt.getClass
      val batchesMethod = clazz.getMethods.find(_.getName == "batches").get
      batchesMethod.setAccessible(true)
      batchesMethod.invoke(opt).asInstanceOf[Seq[AnyRef]]
    }
    def batchNames: Seq[String] =
      batches map { b => b invokePrivate nameMethod() }
  }

  test("No rules is equivalent to DefaultOptimizer") {
    val extOpt = new ExtendableOptimizer()
    val defOpt = DefaultOptimizer
    assert(extOpt.batchNames == defOpt.batchNames)
  }

  test("Early rules are added") {
    val extOpt = new ExtendableOptimizer(
      earlyRules = FiltersReduction :: Nil
    )
    val defOpt = DefaultOptimizer
    assert(extOpt.batchNames.toSet ==
      defOpt.batchNames.toSet ++ Seq("Early extended optimizations"))
  }

  test("Late rules are added") {
    val extOpt = new ExtendableOptimizer(
      lateRules = FiltersReduction :: Nil
    )
    val defOpt = DefaultOptimizer
    assert(extOpt.batchNames == defOpt.batchNames)
  }

  test("Both rules are added") {
    val extOpt = new ExtendableOptimizer(
      earlyRules = FiltersReduction :: Nil,
      lateRules = FiltersReduction :: Nil
    )
    val defOpt = DefaultOptimizer
    assert(extOpt.batchNames.toSet ==
      defOpt.batchNames.toSet ++ Seq("Early extended optimizations"))
  }
}
