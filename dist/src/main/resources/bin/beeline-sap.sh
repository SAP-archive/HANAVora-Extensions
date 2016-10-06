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

#
# Shell script for starting the SQL CLI tool BeeLine (resp. SqlLine)
# The script ensures that further JDBC drivers from the assembly jar
# are contained in the classpath such that access to Vora and HANA is
# possible.

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd -P)
source "$dir/.commons.sh"

# Enter posix mode for bash
set -o posix

# Find the java binary
if [ -n "${JAVA_HOME}" ]; then
  runner="${JAVA_HOME}/bin/java"
else
  runner=`which "$runner"`
fi


run_beeline() {
   exec "$runner" "-Djava.ext.dirs=$SPARK_HOME/lib:$(dirname $spark_ext_lib)" \
       org.apache.hive.beeline.BeeLine "$1"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  run_beeline "--help"
  echo "---"
  echo "additional info at !connect"
  echo "   Vora Url    = jdbc:hive2://<hostname>:<port>"
  echo "        driver = sap.hanavora.jdbc.VoraDriver"
  echo "   HANA Url    = jdbc:sap://<hostname>:3<instance-number>15"
  echo "        driver = com.sap.db.jdbc.Driver"
else
  run_beeline "$@"
fi

