#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Stops the thrift server on the machine this script is executed on.


# check if spark home is set or derive it from path of file spark-submit
if [[ -z $SPARK_HOME ]]; then
  if which spark-submit ; then
    SPARK_HOME="$(cd "`dirname $( readlink -nf $(which spark-submit))`"/..; pwd -P)"
    echo "[INFO] SPARK_HOME is derived from spark-submit path to: $SPARK_HOME"
  else
     echo Error: SPARK_HOME environment variable must be set to Spark installation directory.
    exit 1
  fi
fi

$SPARK_HOME/sbin/spark-daemon.sh stop org.apache.spark.sql.hive.sap.thriftserver.SapThriftServer 1
