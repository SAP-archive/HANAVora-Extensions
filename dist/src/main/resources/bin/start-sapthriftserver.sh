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


dir=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd -P)
source "$dir/.commons.sh"

# Enter posix mode for bash
set -o posix


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

# parse the provided options. result can be found in var trimmed_options
args=( "$@" )
parse_opts "--hiveconf" args[@]

# We don't want some default Hive configuration to be loaded
export HIVE_CONF_DIR=/dev/null

exec "$SPARK_HOME"/bin/spark-submit --class $sapthriftserver_class \
     "${trimmed_options[@]}" $spark_ext_lib --hiveconf spark.sql.hive.thriftServer.singleSession=true "${options[@]}"

