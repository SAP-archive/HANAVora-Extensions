OLAP features
=============

Overview
--------

This page will give an introduction about additional OLAP and metadata support for Spark. It will
describe how to use the various APIs and how to implement them for your own project.

Views
-----

Views are virtual tables that contain the result of a SQL query execution. It is possible to create
views and use them later to construct complex queries. When creating a view in Spark, it will
be deleted after the corresponding SQLContext is closed, however views can also be persisted if
the datasource (e.g. SAP HANA Vora) supports it.

  One remark for persistency: The datasource itself has no notion of the views behavior - When
  a view is stored at a datasource, only the view's SQL-string is stored.

Plain Spark Views
'''''''''''''''''
The user can create a view that resides only in the SQLContext catalog by issuing a
:code:`CREATE VIEW` statement. The basic syntax of :code:`CREATE VIEW` statements looks like this:

.. code-block:: sql

  CREATE (DIMENSION | CUBE)? VIEW <view name>
  AS <view query>

Please note that in the current release :code:`CUBE` and :code:`DIMENSION` views act exactly like
normal views, in later releases they will have extra semantics relevant for OLAP features.

An example of a concrete :code:`CREATE VIEW` statement might look like this:

.. code-block:: sql

  CREATE VIEW MyView
  AS SELECT * FROM Table1


Defining and Using Annotations
------------------------------

The user can define annotations (i.e. a list of key-value pairs) on relation columns. Using these
annotations, external tools such as modelers and visual sql editors can store extra information
about relation columns such as default aggregations or UI formatting tips that make integration
with Spark SQL much easier. In the following example we show how to create a table with
annotations:

.. code-block:: sql

  CREATE VIEW view1 AS
  SELECT col1 AS col1 @(foo = 'bar'),  col2 as col2 @ (baz = 'buzz') FROM table1

In this example we are using a table :code:`table1` which has two columns :code:`col1` and
:code:`col2`. We create a view on these columns and define the annotations :code:`foo -> 'bar'` (:code:`col1`) and
:code:`baz -> 'buzz'` (:code:`col2`). The user can define an arbitrary number of
annotations, however annotations' keys must be unique for a given column, moreover value types can be
either a string, long, double, or a string array. To view the annotations of
the view, the user can execute a table-valued function :code:`DESCRIBE_TABLE` as follows:

.. code-block:: sql

  SELECT * FROM DESCRIBE_TABLE(SELECT * FROM v)


The result will contain all columns and annotations defined on the columns:

============== ============= ============= ================== ============= =========== =================== =============== ================ ================== ========
 TABLE_SCHEMA   TABLE_NAME    COLUMN_NAME   ORDINAL_POSITION   IS_NULLABLE   DATA_TYPE   NUMERIC_PRECISION   NUMERIC_SCALE   ANNOTATION_KEY   ANNOTATION_VALUE   COMMENT
============== ============= ============= ================== ============= =========== =================== =============== ================ ================== ========
 null            t1            col1          0                  true          INTEGER     null                null            foo              bar
 null            t1            col2          1                  true          INTEGER     null                null            baz              buzz
============== ============= ============= ================== ============= =========== =================== =============== ================ ================== ========

Annotations **propagate** through nested views - the following statement will overwrite the annotation
:code:`baz` on column :code:`col2` in view :code:`view1`.

.. code-block:: sql

  CREATE VIEW view2 AS
  SELECT col1 AS col3 @(bar = 'bar'),  col2 as col4 @ (baz = 'overwrite') FROM view1

Note that annotations are persisted inside the datasource. If the datasource does not support
annotations, the annotations are stored within the :code:`SQLContext` and will be deleted upon
closing it.


Table valued functions
----------------------

Table valued functions are functions that can be used in SQL statements
which take logical plans as input and return a table as their output.

The :code:`describe_table` function takes one logical plan and analyzes
all the used relations together with their columns. An example usage of
:code:`describe_table` might look like this:

:code:`SELECT * FROM describe_table(SELECT * FROM t)`

This will return a table with the following fields:

+-------------------------+-------------+----------+-----------------------------------------------------+
| field name              | type        | nullable | description                                         |
+=========================+=============+==========+=====================================================+
| TABLE_SCHEMA            | string      | true     | the schema the relation resides in                  |
+-------------------------+-------------+----------+-----------------------------------------------------+
| TABLE_NAME              | string      | false    | the table name the column belongs to                |
+-------------------------+-------------+----------+-----------------------------------------------------+
| COLUMN_NAME             | string      | false    | the name of the column                              |
+-------------------------+-------------+----------+-----------------------------------------------------+
| ORDINAL_POSITION        | int         | false    | the ordinal position in the relation                |
+-------------------------+-------------+----------+-----------------------------------------------------+
| DATA_TYPE               | string      | false    | the data type of the column                         |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_PRECISION       | int         | true     | the numeric precision in case of numeric type       |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_PRECISION_RADIX | int         | true     | the numeric precision radix in case of numeric type |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_SCALE           | int         | true     | the numeric scale in case of numeric type           |
+-------------------------+-------------+----------+-----------------------------------------------------+
| ANNOTATION_KEY          | string      | true     | the key of an annotation                            |
+-------------------------+-------------+----------+-----------------------------------------------------+
| ANNOTATION_VALUE        | string      | true     | the value of an annotation                          |
+-------------------------+-------------+----------+-----------------------------------------------------+

However, if a target relation does not exist, this will throw an exception.
To have an empty result set if the relation does not exist, there is the
:code:`describe_table_if_exists` function that has the exact same output but does return
an empty result set if the target relation did not exist.

System Tables
-------------

System tables provide a view on the spark and datasource system itself. They can work on standalone
Spark or on a given datasource or also on both. This is individually defined for each system table.

Querying a system table that can work on a given datasource looks as follows:

.. code-block:: sql

  SELECT * FROM SYS.TABLES USING com.sap.spark.vora OPTIONS ()

The :code:`OPTIONS` part is optional. Also, system tables can be accessed via :code:`SYS_`
instead of :code:`SYS`.

If we want to use the same system table but query Spark:

.. code-block:: sql

  SELECT * FROM SYS.TABLES

TABLES System Table
'''''''''''''''''''

The tables system table gives information about the tables contained in
either the spark catalog or the provided datasource. It can be queried via

.. code-block:: sql

  SELECT * FROM SYS.TABLES

or

.. code-block:: sql

  SELECT * FROM SYS.TABLES USING com.sap.spark.vora

The output structure is as follows:

* :code:`TABLE_NAME`: The name of the relation.
* :code:`IS_TEMPORARY`: Whether this table ceases to exist after the current spark context is
  closed or not.
* :code:`KIND`: Either :code:`TABLE` or :code:`VIEW`.
* :code:`PROVIDER`: The provider that has a hold on the relation (or null, if it cannot be
  inferred or there is none).

A sample result looks like this:

============== ====================== ========= =====================
 TABLE_NAME     IS_TEMPORARY           KIND      PROVIDER
============== ====================== ========= =====================
persons          FALSE                  TABLE     com.sap.spark.vora
pets             FALSE                  TABLE     com.sap.spark.vora
pet_owners       TRUE                   VIEW      null
============== ====================== ========= =====================

OBJECT_DEPENDENCIES System Table
''''''''''''''''''''''''''''''''

The object dependencies system table currently works on Spark only. It shows
the direct dependent objects one relation has as well as the type of dependency.
The fields of this system table are:

* :code:`BASE_SCHEMA_NAME`: The schema the base object lies within, usually :code:`null`.
* :code:`BASE_OBJECT_NAME`: The name of the base object, in case of a view or a table this
  is also their name.
* :code:`BASE_OBJECT_TYPE`: The type of the base object. Currently either :code:`TABLE` or :code:`VIEW`.
* :code:`DEPENDENT_SCHEMA_NAME`: The schema the dependent object lies within, usually :code:`null`.
* :code:`DEPENDENT_OBJECT_NAME`: The name of the dependent object.
* :code:`DEPENDENT_OBJECT_TYPE`: The type of the dependent object. Currently either :code:`TABLE` or :code:`VIEW`.
* :code:`DEPENDENTCY_TYPE`: The id of the dependency type. Currently only referential dependency
  with id :code:`0` exists.

    A query to the object dependencies system table looks like this:

.. code-block:: sql

  SELECT * FROM SYS.OBJECT_DEPENDENCIES


A sample result looks like this:

================= ===================== ================== ======================= ======================= ======================= =================
BASE_SCHEMA_NAME   BASE_OBJECT_NAME      BASE_OBJECT_TYPE   DEPENDENT_SCHEMA_NAME   DEPENDENT_OBJECT_NAME   DEPENDENT_OBJECT_TYPE   DEPENDENCY_TYPE
================= ===================== ================== ======================= ======================= ======================= =================
null                persons               table              null                    pets                    table                   0
null                persons               table              null                    english_speakers        view                    0
null                english_speakers      view               null                    presenters              view                    0
================= ===================== ================== ======================= ======================= ======================= =================

METADATA System Table
'''''''''''''''''''''

The metadata system table is provider only. Via this system table, providers can list
provider-specific metadata for a table. It can be queried via

.. code-block:: sql

  SELECT * FROM SYS.TABLE_METADATA USING com.sap.spark.vora

This will retrieve a table with the following fields:

* :code:`TABLE_NAME`: The name of the table the metadata is provided for.
* :code:`METADATA_KEY`: The key of the metadata key value-pair.
* :code:`METADATA_VALUE`: The value of the metadata key value-pair.

Both :code:`METADATA_KEY` and :code:`METADATA_VALUE` are strings. The Vora datasource returns
JSON strings for some :code:`METADATA_VALUE`s.

Sample output can be seen below:

============ ============== ===============
 TABLE_NAME   METADATA_KEY   METADATA_VALUE
============ ============== ===============
employees     parent         null
employees     version        1.2.5
============ ============== ===============

SCHEMA System Table
'''''''''''''''''''

The schema system table is available on both vora and on providers
implementing the :code:`DatasourceCatalog` interface. It queries for all
the table schemas of the target provider. A SQL statement looks like this:

.. code-block:: sql

  SELECT * FROM SYS.SCHEMAS [ USING com.sap.spark.vora ]


(The "[ ... ]" stands for optional).
This will yield a table with the fields below:

* :code:`TABLE_SCHEMA`: The schema the table resides in.
* :code:`TABLE_NAME`: The name of the table.
* :code:`COLUMN_NAME`: The name of the column.
* :code:`ORDINAL_POSITION`: The ordinal position of the column in the table schema.
* :code:`IS_NULLABLE`: A boolean whether the column value is nullable or not.
* :code:`DATA_TYPE`: The data type of the given table. May be a datasource dependent native type.
* :code:`SPARK_TYPE`: The corresponding Spark type for the previous data type. Might be null if there is none.
* :code:`NUMERIC_PRECISION`: The numeric precision of the spark data type, if it is a numeric type.
* :code:`NUMERIC_PRECISION_RADIX`: The numeric precision radix of the spark data type, if it is a numeric type.
* :code:`NUMERIC_SCALE`: The numeric scale of the spark data type, if it is a numeric type.
* :code:`ANNOTATION_KEY`: The key of a column annotation.
* :code:`ANNOTATION_VALUE`: The value of a column annotation.
* :code:`COMMENT`: A comment attached to the column.

Sample output can be seen below.

============= ============ ============= ================== ============= =============== ============ =================== ========================= =============== ================ ================== =========
TABLE_SCHEMA   TABLE_NAME   COLUMN_NAME   ORDINAL_POSITION   IS_NULLABLE   DATA_TYPE       SPARK_TYPE   NUMERIC_PRECISION   NUMERIC_PRECISION_RADIX   NUMERIC_SCALE   ANNOTATION_KEY   ANNOTATION_VALUE   COMMENT
============= ============ ============= ================== ============= =============== ============ =================== ========================= =============== ================ ================== =========
 null          persons       name          1                  false         VARCHAR(20)     string       null                null                      null            meta             data              comment1
 null          animals       age           1                  false         INTEGER         int          32                  2                         0               null             null
============= ============ ============= ================== ============= =============== ============ =================== ========================= =============== ================ ================== =========

Infer schema command
''''''''''''''''''''

The :code:`INFER SCHEMA` command can be used to retrieve the schema of an ``.orc`` or a ``.parquet`` file.
The syntax of the command is as follows:

.. code-block::

  INFER SCHEMA OF "<path to file>" [ AS ( PARQUET | ORC ) ]

If you do not specify the ``AS ...`` clause, the file extension will be used to infer the file type.
If the file type still cannot be inferred, an exception is thrown.
The path to the file may either be a local path or an HDFS path (``hdfs://``-prefix).

An example invocation of the :code:`INFER SCHEMA` command might look like this:

.. code-block::

  INFER SCHEMA OF "/persons.orc"

:code:`INFER SCHEMA` always yields the following columns as result:

+-------------------------+-------------+----------+-----------------------------------------------------+
| field name              | type        | nullable | description                                         |
+=========================+=============+==========+=====================================================+
| COLUMN_NAME             | string      | false    | the name of the inferred column                     |
+-------------------------+-------------+----------+-----------------------------------------------------+
| ORDINAL_POSITION        | int         | false    | the ordinal position of the column                  |
+-------------------------+-------------+----------+-----------------------------------------------------+
| DATA_TYPE               | string      | false    | the data type of the column                         |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_PRECISION       | int         | true     | the numeric precision in case of numeric type       |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_PRECISION_RADIX | int         | true     | the numeric precision radix in case of numeric type |
+-------------------------+-------------+----------+-----------------------------------------------------+
| NUMERIC_SCALE           | int         | true     | the numeric scale in case of numeric type           |
+-------------------------+-------------+----------+-----------------------------------------------------+

