package org.apache.spark.sql.extension

import org.apache.spark.sql.catalyst.optimizer.{FiltersReduction, Optimizer}
import org.apache.spark.sql.extension.OptimizerFactory.ExtendableOptimizerBatch
import org.scalatest.{FunSuite, PrivateMethodTester}

class ExtendableOptimizerSuite extends FunSuite with PrivateMethodTester {

  implicit class OptimizerOps(opt: Optimizer) {
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
    val extOpt = OptimizerFactory.produce()
    val defOpt = OptimizerFactoryForTests.default()
    assert(extOpt.batchNames == defOpt.batchNames)
  }

  test("One early batch is added before the main optimizer batch") {
    val extOpt = OptimizerFactory.produce(
      earlyBatches = ExtendableOptimizerBatch("FOO", 1, FiltersReduction :: Nil) :: Nil
    )

    assert(extOpt.batchNames match {
      case subQueries :: early :: other => early.equals("FOO")
    })
  }

  test("Several early batches are added before the main optimizer batch") {
    val extOpt = OptimizerFactory.produce(
      earlyBatches = ExtendableOptimizerBatch("FOO", 1, FiltersReduction :: Nil) ::
        ExtendableOptimizerBatch("BAR", 1, FiltersReduction :: Nil) ::
        Nil
    )

    assert(extOpt.batchNames match {
      case subQueries :: firstEarly :: secondEarly :: other =>
        firstEarly.equals("FOO") && secondEarly.equals("BAR")
    })
  }

  test("Expression rules are added") {
    val extOpt = OptimizerFactory.produce(
      mainBatchRules = FiltersReduction :: Nil
    )
    val defOpt = OptimizerFactoryForTests.default()
    assert(extOpt.batchNames == defOpt.batchNames)
  }

  test("Both rules are added") {
    val extOpt = OptimizerFactory.produce(
      earlyBatches = ExtendableOptimizerBatch("FOO", 1, FiltersReduction :: Nil) :: Nil,
      mainBatchRules = FiltersReduction :: Nil
    )
    val defOpt = OptimizerFactoryForTests.default()
    assert(extOpt.batchNames.toSet ==
      defOpt.batchNames.toSet ++ Seq("FOO"))
  }
}
