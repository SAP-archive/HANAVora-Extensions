/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package corp.sap.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, Suite}

/**
 * Shares a local `SparkContext` between all tests in a suite and closes it at the end.
 */
trait GlobalSparkContext extends WithSparkContext {
  self: Suite =>

  override def sc: SparkContext = GlobalSparkContext._sc

  override protected def setUpSparkContext(): Unit = {
    GlobalSparkContext.init(numberOfSparkWorkers, sparkConf)
  }

  override protected def tearDownSparkContext(): Unit = {
    GlobalSparkContext.reset()
    /* Do not tear down context */
  }

}

object GlobalSparkContext {
  @transient private var _sc: SparkContext = _

  def init(numberOfSparkWorkers: Int, sparkConf: SparkConf): Unit = {
    if (_sc == null) {
      this.synchronized {
        if (_sc == null) {
          _sc = new SparkContext(s"local[$numberOfSparkWorkers]", "test", sparkConf)
        }
      }
    }
  }

  def reset(): Unit = {
    _sc.cancelAllJobs()
  }

  def close(): Unit = {
    if (_sc != null) {
      _sc.stop()
      _sc = null
    }
  }

}
