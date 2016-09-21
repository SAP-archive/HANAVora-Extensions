Scripts
#######

This chapter contains the documentation of the scripts available in the SAP HANA Vora Spark
extension library.

Overview
========

The scripts can be found in the resources folder of the ``dist`` module. After packaging, they
are available in the tarball's ``bin`` directory.

===================================  ===============================================================
Script                               Description
===================================  ===============================================================
``start-spark-shell.sh``             Spawn a spark shell that includes the Vora library
                                     automatically.
``run-spark-sql.sh``                 Execute one or more SQL scripts, outputting either to stdout or
                                     to file.
===================================  ===============================================================

Common Functionality
====================

The scripts wrapping ``spark-submit.sh`` or ``spark-shell.sh`` hand all additional parameters
down to the wrapped script. One of these parameters deserves special mention: in production or
verification environments, the default parameter ``--master=local`` almost always is not what
the user wants!  It should be set to the explicit cluster manager, i.e. mesos, yarn or standalone.


Spark Shell
===========

This script is a wrapper for the ``spark-shell.sh`` script included in the Spark distributions.
It adds the ``spark-sap-parent-<version>-assembly.jar`` to the classpath and hands all options
(other than ``-h|--help``) down to ``spark-shell.sh``.


Spark SQL CLI
=============

This script provides a command line interface to the ``SapSQLContext.sql()`` method.

Arguments
---------

The script takes 1 or more files as positional arguments and either writes output to the terminal or
saves it to disk. Commands must be separated by semicolons ``;``, and comments - starting with
double dashes ``--`` and extending to the end of line - are ignored.

==================  ================================================================================
Option              Description
==================  ================================================================================
-h                  Print usage information and exit.
-o <file>           Redirect output to <file> (default: write to stdout)
==================  ================================================================================

Error Handling
--------------

The script will execute SQL commands from the given files until an error occurs. If a SQL call leads
to an error, the spark executor prints its stack trace and exits. If a file passed on the command
line does not exist, the script prints an error message and returns a non-zero value.