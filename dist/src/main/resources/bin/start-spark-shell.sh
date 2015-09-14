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

: ${SPARK_HOME:?"Need to set SPARK_HOME non-empty"}

SPARKBIN=$SPARK_HOME/bin
BASE_DIR="$(dirname $0)/../"
cd $BASE_DIR


$SPARKBIN/spark-shell --jars lib/*.jar $*
