#!/usr/bin/env bash

function usage {
  echo -en "Usage: $0 input.sql [input2.sql ...] [-o output.csv]

Runs all commands in the given SQL script files. Note that output will be in a
simple CSV format without quotation.

Options:

You can use all options for 'spark-submit' (besides '--class').

NOTE: You probably need to set the option '--master' to something adequate for your cluster.

Additional options:

-h\tprint this message
-o\toutput file (default: stdout)
"
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

dir=$(cd "$(dirname "${BASH_SOURCE[0]}")"; pwd -P)
source "$dir/.commons.sh"

exec $SPARK_HOME/bin/spark-submit --class com.sap.spark.cli.SQLRunner "$spark_ext_lib" $*
