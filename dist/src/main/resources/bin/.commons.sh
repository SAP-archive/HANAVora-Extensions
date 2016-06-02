#!/usr/bin/env bash

script=`readlink -nf "${BASH_SOURCE[0]}"`
dir=`dirname "$script"`
lib_dir=`readlink -fn "$dir"/../lib`


function set_spark_home {
    # check if spark home is set or derive it from path of file spark-submit
    if [[ -z $SPARK_HOME ]]; then
        spark_submit_binary=`readlink -nf "$(which spark-submit)"`
        if [[ ! -z "$spark_submit_binary" ]]
        then
            export SPARK_HOME=`dirname "$spark_submit_binary"`
            echo "[INFO] SPARK_HOME is derived from spark-submit path to: $SPARK_HOME"
        else
            echo "[Error] SPARK_HOME environment variable must be set to Spark installation directory"
            exit 1
        fi
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
    user_args=""
    while [[ $# > 0 ]]
    do
        key="$1"

        case $key in
        --jars)
        user_jars="$2"
        shift # skip the value part
        ;;
        *)
        user_args+=" \"$1\""
        ;;
        esac
        shift
    done
}

set_spark_home

set_spark_libs


