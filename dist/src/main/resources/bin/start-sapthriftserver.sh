#!/usr/bin/env bash

# Enter posix mode for bash
set -o posix

# Get the current directory
FWDIR="$(cd "`dirname "$0"`"/..; pwd)"

# check if spark home is set
if [[ -z $SPARK_HOME ]]; then
  echo Error: SPARK_HOME environment variable must be set to Spark installation directory.
  exit 1
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
  SPARK_VELOCITY_ASSEMBLY_DIR=$FWDIR/target
elif [[ -e $FWDIR/lib ]]; then
  SPARK_VELOCITY_ASSEMBLY_DIR=$FWDIR/lib
else
  echo Error: Spark Velocity assembly directory is not found.
  exit 1
fi


SPARK_VELOCITY_ASSEMBLY_JAR=
export SUBMIT_USAGE_FUNCTION=usage

num_jars="$(ls -1 "$SPARK_VELOCITY_ASSEMBLY_DIR" | grep "^spark-sap-.*-assembly\.jar$" | wc -l)"
if [ "$num_jars" -eq "0" -a -z "$SPARK_ASSEMBLY_JAR" ]; then
  echo "Failed to find Spark SAP assembly in $SPARK_VELOCITY_ASSEMBLY_DIR." 1>&2
  echo "You need to build Spark SAP extensions before running this program." 1>&2
  exit 1
fi

ASSEMBLY_JARS="$(ls -1 "$SPARK_VELOCITY_ASSEMBLY_DIR" | grep "^spark-sap-.*-assembly\.jar$" || true)"

if [ "$num_jars" -gt "1" ]; then
  echo "Found multiple Spark assembly jars in $SPARK_VELOCITY_ASSEMBLY_DIR:" 1>&2
  echo "$ASSEMBLY_JARS" 1>&2
  echo "Please remove all but one jar." 1>&2
  exit 1
fi


SPARK_VELOCITY_ASSEMBLY_JAR=${SPARK_VELOCITY_ASSEMBLY_DIR}/${ASSEMBLY_JARS}

exec "$SPARK_HOME"/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.SapThriftServer "$@" $SPARK_VELOCITY_ASSEMBLY_JAR
