#!/usr/bin/env bash

function usage {
  echo "Usage: $0 <commands.sql> (-o output.sql | -t expected_output.sql)\n"
  pattern="\n"
  pattern+="\|Runs all commands in the given SQL script file.\n"
  pattern+="\|One of the flags '-o' or '-t' must be provided.\n"
  pattern+="\|=======\n"
  pattern+="\|-h/--help  : Prints out this text\n"
  pattern+="\|-o <file>  : Write the output to <file>\n"
  pattern+="\|-t <file>  : Compare the output to <file>\n"
  echo -e $pattern
}

if [[ "$@" = *--help ]] || [[ "$@" = *-h ]]; then
  usage
  exit 0
fi

# check if spark home is set or derive it from path of file spark-shell
if [[ -z $SPARK_HOME ]]; then
  if which spark-submit ; then
    SPARK_HOME="$(cd "`dirname $( readlink -nf $(which spark-submit))`"/..; pwd -P)"
    echo "[INFO] SPARK_HOME is derived from spark-shell path to: $SPARK_HOME"
  else
     echo Error: SPARK_HOME environment variable must be set to Spark installation directory.
    exit 1
  fi
fi

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
  jar="`ls \"$lib_dir\"/*.jar`"
fi

$SPARK_HOME/bin/spark-submit --class com.sap.spark.SQLRunner "$jar" $*
