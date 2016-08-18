/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sap.zeppelin.spark;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SapSQLContext;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.spark.SparkInterpreter;
import org.apache.zeppelin.spark.SparkSqlInterpreter;
import org.apache.zeppelin.spark.ZeppelinContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sap SQL interpreter for Zeppelin.
 */
public class SapSqlInterpreter extends SparkSqlInterpreter{
  private final Logger logger = LoggerFactory.getLogger(SapSqlInterpreter.class);

  // Please use "getSapSQLContext()" as a getter
  private SapSQLContext _sapSqlC = null;

  private int maxResult;

  private String getJobGroup(InterpreterContext context){
    return "zeppelin-" + context.getParagraphId();
  }

  public SapSqlInterpreter(Properties property) {
    super(property);
    this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));
  }

  /**
   * This method originally is from SparkSqlInterpreter (Zeppelin 0.6.1) we keep it as it is
   *
   * @return
     */
  private SparkInterpreter getSparkInterpreter() {
    LazyOpenInterpreter lazy = null;
    SparkInterpreter spark = null;
    Interpreter p = getInterpreterInTheSameSessionByClassName(SparkInterpreter.class.getName());

    while (p instanceof WrappedInterpreter) {
      if (p instanceof LazyOpenInterpreter) {
        lazy = (LazyOpenInterpreter) p;
      }
      p = ((WrappedInterpreter) p).getInnerInterpreter();
    }
    spark = (SparkInterpreter) p;

    if (lazy != null) {
      lazy.open();
    }
    return spark;
  }

  public synchronized SapSQLContext getSapSQLContext() {
    SparkInterpreter sparkInterpreter = getSparkInterpreter();
    // there is no SapSqlContext instantiated, we create a new one
    if(this._sapSqlC == null) {
      logger.debug("No SapSqlContext instantiated, creating a new one");
      _sapSqlC = new SapSQLContext(sparkInterpreter.getSparkContext());
    }

    return _sapSqlC;
  }

  @Override
  public InterpreterResult interpret(String st, InterpreterContext context) {
    logger.debug("Calling: SapSQLInterpreter with " + st + " on context " + context.toString());

    SapSQLContext sqlc = getSapSQLContext();

    SparkContext sc = sqlc.sparkContext();
    if (concurrentSQL()) {
      sc.setLocalProperty("spark.scheduler.pool", "fair");
    } else {
      sc.setLocalProperty("spark.scheduler.pool", null);
    }

    sc.setJobGroup(getJobGroup(context), "Zeppelin", false);

    if(sqlc.DATASOURCES_VERSION().isDefined()) {
      logger.debug("About to execute on SapSQLContext with Version "
              + sqlc.DATASOURCES_VERSION().get());
    } else {
      logger.debug("About to execute on SapSQLContext with Unknown Version");
    }

    Object rdd = null;
    try {
      rdd = sqlc.sql(st);
    } catch (Exception e) {
      if (Boolean.parseBoolean(getProperty("zeppelin.spark.sql.stacktrace"))) {
        throw new InterpreterException(e);
      }
      logger.error("Exception during SQL Processing (SapSqlContext)", e);
      String msg = e.getMessage()
              + "\nset zeppelin.spark.sql.stacktrace = true to see full stacktrace";
      return new InterpreterResult(Code.ERROR, msg);
    }

    String msg = ZeppelinContext.showDF(sc, context, rdd, this.maxResult);
    sc.clearJobGroup();
    return new InterpreterResult(Code.SUCCESS, msg);
  }

}
