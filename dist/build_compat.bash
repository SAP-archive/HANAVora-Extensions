#!/bin/bash
#
# Builds the project with Spark 1.5 compatibility.
#
# Run from the root of the project.
#

silent_mvn() {
  cmd="mvn $@" > /dev/null
  echo "$cmd"
  $cmd
}

get_version() {
  grep "<version>" pom.xml | head -n 1 | sed -e 's|.*<version>\(.*\)</version>.*|\1|g'
}

COMPAT_SUFFIX="spark-1.5"
VERSION="$(get_version)"
COMPAT_VERSION="${VERSION}_$COMPAT_SUFFIX"
echo "Current version: $VERSION"
echo "Compat version: $COMPAT_VERSION"

silent_mvn versions:set -DoldVersion=$VERSION -DnewVersion=$COMPAT_VERSION

echo "Building..."
mvn -P$COMPAT_SUFFIX clean package install

silent_mvn versions:revert
