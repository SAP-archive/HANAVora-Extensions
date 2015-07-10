#!/usr/bin/env bash

# starts a spark shell with the distribution assembly



function usage {
  echo "Usage: ./bin/start-spark-shell [spark options]\n"
  pattern="usage\n"
  pattern+="\|Script includes all the libraries in the lib folder and starts a spark shell\n"
  pattern+="\|The SPARK_HOME environment variable has to be set!\n"
  pattern+="\|NOTE: you can add additional parameters for spark as well\n"
  partern+="\|Parameters for this script\n"
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
