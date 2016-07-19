#!/usr/bin/env bash

script=`readlink -nf "${BASH_SOURCE[0]}"`
dir=`dirname "$script"`
lib_dir=`readlink -fn "$dir"/../lib`


function check_spark_home {
    # check if spark home is set or fail
    if [[ -z $SPARK_HOME ]]; then
        echo "[Error] SPARK_HOME environment variable must be set to Spark installation directory"
        exit 1
    fi
}


function set_spark_libs {
    num_libs=`ls "$lib_dir"/spark-sap-*.jar 2>/dev/null|wc -l`

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
        spark_ext_lib=`ls "$lib_dir"/spark-sap-*.jar`
    fi
}


function parse_user_jars {
    # parse user-provided options
    user_jars=""
    user_args=()
    while [[ $# > 0 ]]
    do
        key="$1"

        case $key in
        --jars)
        user_jars="$2"
        shift # skip the value part
        ;;
        *)
        user_args+=($key)
        ;;
        esac
        shift
    done
}

check_spark_home

set_spark_libs


