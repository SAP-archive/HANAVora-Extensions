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
  echo -en "Usage: $0 [spark-shell options]

Script includes the jar library from the lib folder and starts a spark shell.

Options:

You can use all options for 'spark-shell' even '--jars'). Additional options:

-h\tprint this message
"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

script=`readlink -f "${BASH_SOURCE[0]}"`
dir=`dirname "$script"`
source "$dir/.commons.sh"

parse_user_jars $@

jars="$spark_ext_lib,$user_jars"




exec $SPARK_HOME/bin/spark-shell --jars "$jars" "${user_args[@]}"


