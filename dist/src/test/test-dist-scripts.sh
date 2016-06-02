#!/usr/bin/env bash

# This testing script is intended for manual testing of the bash scripts in dist/.
# It uses mock implementations of spark scripts as a smoke test, but does not
# check their actual behavior.

script="`readlink -f \"${BASH_SOURCE[0]}\"`"
dir="`dirname \"$script\"`"

bin_dir="`readlink -fn $dir/../main/resources/bin`"
lib_dir="`readlink -fn $dir/../main/resources/lib`"


function set_fake_spark_home {
    export SPARK_HOME="$dir"
    fake_spark_bin="$SPARK_HOME"/bin

    if [[ -d "$fake_spark_bin" ]]
    then
        echo "not overwriting existing directory $fake_spark_bin"
        cleanup_spark_dir=0
    else
        echo "creating $fake_spark_bin"
        mkdir -p "$fake_spark_bin"
        for f in spark-shell spark-submit
        do
            echo '#!/bin/bash' >"$fake_spark_bin"/$f
            echo 'echo "$0" "$@"' >>"$fake_spark_bin"/$f
            chmod u+x "$fake_spark_bin"/$f
        done
        cleanup_spark_dir=1
    fi
}


function set_fake_lib_dir {
    if [[ -d "$lib_dir" ]]
    then
        echo "not overwriting existing directory $lib_dir"
        cleanup_lib_dir=0
    else
        echo "creating $lib_dir"
        mkdir -p "$lib_dir"
        for f in some.jar other.jar instrumetation-app.jar spark-sap-datasources-1.3.0-assembly.jar
        do
            touch "$lib_dir/$f"
        done
        cleanup_lib_dir=1
    fi
}


function cleanup {
    if [[ $cleanup_lib_dir > 0 ]]
    then
        echo "cleaning up $lib_dir"
        rm -f "$lib_dir"/* 2>/dev/null
        rmdir "$lib_dir"
    fi
    if [[ $cleanup_spark_dir > 0 ]]
    then
        echo "cleaning up $fake_spark_bin"
        rm -f "$fake_spark_bin"/* 2>/dev/null
        rmdir "$fake_spark_bin"
    fi
}


function fail {
    cleanup
    echo "*** FAILED TEST ***"
    exit 1
}

set_fake_spark_home || fail
set_fake_lib_dir || fail

echo "Testing scripts in" "$bin_dir"

cmd="start-spark-shell.sh"

echo ""
echo "===================================="
echo "Trying $cmd"
echo ""

"$bin_dir/$cmd" -h || fail
"$bin_dir/$cmd" || fail
"$bin_dir/$cmd" --jars a.jar,b.jar || fail
"$bin_dir/$cmd" --jars a.jar,b.jar additional options || fail

cmd="run-spark-sql.sh"

echo ""
echo "===================================="
echo "Trying $cmd"
echo ""

"$bin_dir/$cmd" -h || fail
"$bin_dir/$cmd" || fail
"$bin_dir/$cmd" --jars a.jar,b.jar || fail

echo ""
echo "All tests passed."
cleanup
