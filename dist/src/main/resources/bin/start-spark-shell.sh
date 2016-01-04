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

# starts a spark shell with the distribution assembly

export FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

function usage {
  echo "Usage: ./bin/start-spark-shell [spark options]\n"
  pattern="usage\n"
  pattern+="\|Script includes all the libraries in the lib folder and starts a spark shell\n"
  pattern+="\|The SPARK_HOME environment variable has to be set!\n"
  pattern+="\|NOTE: you can add additional parameters for spark as well\n"
  pattern+="\|Parameters for this script\n"
  pattern+="\|=======\n"
  pattern+="\|--help : Prints out this text\n"
  echo -e $pattern
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

# check if spark home is set or derive it from path of file spark-shell
if [[ -z $SPARK_HOME ]]; then
  if which spark-shell ; then
    SPARK_HOME="$(cd "`dirname $( readlink -nf $(which spark-shell))`"/..; pwd -P)"
    echo "[INFO] SPARK_HOME is derived from spark-shell path to: $SPARK_HOME"
  else
     echo Error: SPARK_HOME environment variable must be set to Spark installation directory.
    exit 1
  fi
fi

$SPARK_HOME/bin/spark-shell --jars "$FWDIR"/lib/*.jar $*
