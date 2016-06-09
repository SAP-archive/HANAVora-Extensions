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

# Enter posix mode for bash
set -o posix

# Get the current directory
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

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


function usage {
  echo "Usage: ./bin/start-sapthriftserver [options] [thrift server options]"
  pattern="usage"
  pattern+="\|Spark assembly has been built with Hive"
  pattern+="\|NOTE: SPARK_PREPEND_CLASSES is set"
  pattern+="\|Spark Command: "
  pattern+="\|======="
  pattern+="\|--help"

  "$SPARK_HOME"/bin/spark-submit --help 2>&1 | grep -v Usage 1>&2
  echo
  echo "Thrift server options:"
  "$SPARK_HOME"/bin/spark-class $CLASS --help 2>&1 | grep -v "$pattern" 1>&2
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi


if [[ -e $FWDIR/target ]]; then
  SPARK_VORA_ASSEMBLY_DIR=$FWDIR/target
elif [[ -e $FWDIR/lib ]]; then
  SPARK_VORA_ASSEMBLY_DIR=$FWDIR/lib
else
  echo Error: Spark Vora assembly directory is not found.
  exit 1
fi


SPARK_VORA_ASSEMBLY_JAR=
export SUBMIT_USAGE_FUNCTION=usage

num_jars="$(ls -1 "$SPARK_VORA_ASSEMBLY_DIR" | grep "^spark-sap-.*-assembly\.jar$" | wc -l)"
if [ "$num_jars" -eq "0" -a -z "$SPARK_ASSEMBLY_JAR" ]; then
  echo "Failed to find Spark SAP assembly in $SPARK_VORA_ASSEMBLY_DIR." 1>&2
  echo "You need to build Spark SAP extensions before running this program." 1>&2
  exit 1
fi

ASSEMBLY_JARS="$(ls -1 "$SPARK_VORA_ASSEMBLY_DIR" | grep "^spark-sap-.*-assembly\.jar$" || true)"

if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Spark assembly jars in $SPARK_VORA_ASSEMBLY_DIR:" 1>&2
  echo "$ASSEMBLY_JARS" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi


SPARK_VORA_ASSEMBLY_JAR=${SPARK_VORA_ASSEMBLY_DIR}/${ASSEMBLY_JARS}

exec "$SPARK_HOME"/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.SapThriftServer \
     "$@" $SPARK_VORA_ASSEMBLY_JAR --hiveconf spark.sql.hive.thriftServer.singleSession=true
