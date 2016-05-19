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
  pattern+="\|Script includes the jar library from the lib folder and\n"
  pattern+="\|starts a spark shell.\n"
  pattern+="\|The SPARK_HOME environment variable has to be set!\n"
  pattern+="\|NOTE: you can add additional parameters for spark as well\n"
  pattern+="\|\n"
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



# parse user-provided options
user_args=""
while [[ $# > 0 ]]
do
  key="$1"

  case $key in
    --jars)
      user_jars="$2"
      shift # skip the value part
    ;;
    *)
      user_args+=" $1"
    ;;
  esac
  shift
done


# add spark-sap-datasource jar to --jars
lib_dir="`dirname $0`/../lib"
num_libs=`ls "$lib_dir"/*.jar 2>/dev/null|wc -l`

if [[ $num_libs != 1 ]]
then
  if [[ $num_libs == 0 ]]
  then
    msg="no jar file found in $lib_dir"
  else
    msg="too many jar files in $lib_dir"
  fi
  echo "[ERROR] $msg"
  echo "[ERROR] please check your installation"
  exit 1
else
  # trailing comma is ok
  jars="`ls \"$lib_dir\"/*.jar`,$user_jars"
fi

$SPARK_HOME/bin/spark-shell --jars $jars $user_args


