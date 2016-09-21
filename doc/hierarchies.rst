Spark Hierarchies
=================

Hierarchical data structures define a parent-child relationship between different data items, this
abstraction makes it possible to perform complex computations on different levels of data. For
example an organization is basically a hierarchy where the connections between nodes (e.g. middle
manager, developer) are determined by the managerial responsibilities that are defined by the
organization itself.

Using standard SQL to work with hierarchical data and perform analysis on
this data is very difficult. Therefore we extend Spark SQL with hierarchical queries which makes
it possible to define a hierarchy data frame and perform custom hierarchy UDFs on it. For example
it is possible to define a hierarchy of an organization and perform complex aggregations like
calculating the average age of all second level managers or the aggregate salaries of different
departments.

Overview
--------

Our contribution to Spark regarding hierarchies involves:

Extending the parser with hierarchy syntax that allows the user to define a hierarchy, we also
added a list of UDFs to perform calculation on hierarchy tables (e.g. :code:`IS_ROOT` which returns
true if a row in a hierarchy table is actually a root of a tree).

Added two strategies to generate hierarchical data frames, one that uses self joins and another one
that uses broadcasting a hierarchy structure.

In the following we provide a basic overview on how to use the hierarchies in Spark.

Adjacency-List Hierarchy
------------------------

A hierarchy is built using an adjacency list which defines the edges between hierarchy nodes. The
adjacency list is read from a source table where each row of the source table becomes a node in the
hierarchy. The hierarchy SQL syntax allows the developer to define the adjacency list and tweak it.
It also makes it possible to tweak the creation of the hierarchy by controlling the order of the
children of each node and explicitly determining the roots of the hierarchy.

Let’s assume we have a h_src relation containing two columns: :code:`pred` and :code:`succ` which
correspond to predecessor and successor, respectively. The following statement can be used to
create and query a hierarchy:

.. code-block:: sql

  SELECT name, IS_ROOT(node) FROM HIERARCHY (
  USING h_src AS v
  JOIN PRIOR u ON v.pred = u.succ
  ORDER SIBLINGS BY ord ASC
  START WHERE pred IS NULL
  SET node
  ) AS H

The :code:`JOIN PRIOR <alias> <equality exp>` clause defines the construction of the adjacency
list, in this example it states that two rows of :code:`h_src` have and edge between them in the
hierarchy if the child row's :code:`pred` column is equal to the parent row's :code:`succ` column.
The clause is necessary.

The :code:`ORDER SIBLINGS BY <order_by_expression>` is used to determine the order of the children
while constructing the hierarchy. The order is relevant for some UDFs such as :code:`IS_PRECEDING`
and :code:`IS_FOLLOWING` and it can be defined by the user. This clause is optional, if it is not
defined then using UDFs that depend on children order results in undefined results.

The :code:`START WHERE <condition>` defines the roots of the hierarchy. Any row that matches the
condition will be considered a root in the hierarchy forest. This clause is also optional and if it
is not defined by the user then root(s) of the hierarchy will be determined by scanning all
source table rows and looking for the ones that do not have a parent.

The :code:`SET <node column name>` names the newly created node column [#node]_; The created hierarchy
relation has the same schema of :code:`h_src` but in addition it contains an extra column named (in
the above query it is called node) which is specific to the hierarchical operations. This column is
internal, for each row in the hierarchy relation this column will contain information necessary to
specify its location in the hierarchy. One important remark about the column is that it may not be
used in top level query unless it is inside a hierarchy UDF.

Level-Based Hierarchy
---------------------

An alternative way of creating a hierarchy is by mapping the hierarchy levels to source table
columns. The type of hierarchy creation is especially useful when the source table is actually a
flattened hierarchy where all hierarchy paths are encoded as rows.

Let's assume our source table has the following schema: :code:`h_src(col1, col2, col3, col4)` where
:code:`col1` represents the first level, :code:`col2` represents the second level and so on. Using
this table we can create a level-based hierarchy as the following:

.. code-block:: sql

  SELECT name, IS_ROOT(node) FROM HIERARCHY (
  USING h_src WITH LEVELS (col1, col2, col3, col4)
  MATCH PATH
  ORDER SIBLINGS BY ord ASC
  SET node) AS H) AS H

The :code:`WITH LEVELS (col1, col2, col3, col4)` is used to determine the levels of the hierarchy,
in this example it means that we should :code:`col1` for the first level, :code:`col2` for the
second level and so on.

The :code:`MATCH PATH` clause is used to determine the way we handle identical nodes across the
same and across different columns. [#matchers]_

The :code:`ORDER SIBLINGS BY` clause has the same semantics as in adjacency-list hierarchies.

The :code:`SET` clause also has the same semantics as in adjacency-list hierarchies.


Hierarchy UDFs
--------------

Here is a list of UDFs that can be used with hierarchies:

====================================== ===============================================================
UDF                                     Description
====================================== ===============================================================
:code:`LEVEL(u)`                        returns the level of the node in the tree
:code:`IS_ROOT(u)`                      true if the node is root, otherwise false
:code:`IS_DESCENDANT(u,v)`              true if node u is descendant of node v
:code:`IS_DESCENDANT_OR_SELF(u,v)`      u = v or :code:`IS_DESCENDANT(u,v)`
:code:`IS_ANCESTOR(u,v)`                true if node u is ancestor of node v
:code:`IS_ANCESTOR_OR_SELF(u,v)`        u = v or :code:`IS_ANCESTOR(u,v)`
:code:`IS_PARENT(u,v)`                  true if node u is parent of node v
:code:`IS_CHILD(u,v)`                   true if node u is child of node v
:code:`IS_SIBLING(u,v)`                 true if node u is sibling of node v
:code:`IS_FOLLOWING(u,v)`               true if node u follows node v in preorder and is not descendant of v
:code:`IS_PRECEDING(u,v)`               true if node u precedes node v in preorder and is not descendant of v
:code:`NODE(node)`                      returns the textual name of the node.
====================================== ===============================================================

Hierarchy Example:
''''''''''''''''''

In this example we assume we have the following source table :code:`h_src` that represents a simple
organizational hierarchy:

========================= ========== =========== =========
 name                      pred       succ        ord
========================= ========== =========== =========
Chief Executive Officer    None        1           1
Project Manager            1           2           1
Sales Manager              1           3           2
Project Coordinator        2           4           1
Architect                  2           5           2
Programmer                 4           6           1
Designer                   4           7           2
========================= ========== =========== =========

Using equality between :code:`pred` and :code:`succ` the above table represents the hierarchy we
depict graphically below:

.. code-block:: raw

                                                         +-------------------------+
                                                         |                         |
                                                         | Chief Executive Officer |
                                                         |                         |
                                                +--------+-------------------------+--------+
                                                |                                           |
                                                |                                           |
                                       +--------v--------+                         +--------v------+
                                       |                 |                         |               |
                                       | Project Manager |                         | Sales Manager |
                                       |                 |                         |               |
                            +----------+-----------------+-----+                   +---------------+
                            |                                  |
                            |                                  |
                 +----------v----------+                 +-----v-----+
                 |                     |                 |           |
                 | Project Coordinator |                 | Architect |
                 |                     |                 |           |
          +------+---------------------+-----+           +-----------+
          |                                  |
          |                                  |
    +-----v------+                      +----v-----+
    |            |                      |          |
    | Programmer |                      | Designer |
    |            |                      |          |
    +------------+                      +----------+


To complete the example, let's also assume we have another table called :code:`t_src` that holds
the addresses of some employees in the organization:

========================= ============================
 name                      address
========================= ============================
Chief Executive Officer    25 Park Lane, London
Project Manager            20 Euston Road, London
Project Coordinator        12 Abbey Road, London
Designer                   5 Carnaby Street, London
Consultant                 16 Portobello Road, London
========================= ============================

To get the name, address and the level of the employees in the tree we can execute the following
statement:

.. code-block:: sql

  SELECT B.name, A.address, B.level
  FROM
  (SELECT name, LEVEL(node) AS level FROM HIERARCHY (
  USING h_src AS v
  JOIN PRIOR u ON v.pred = u.succ
  ORDER SIBLINGS BY ord ASC
  START WHERE pred IS NULL
  SET node)
  AS H) B, t_src A
  WHERE B.name = A.name

The result of this query will be:

========================= ========================== ===========
 name                      address                    level
========================= ========================== ===========
Chief Executive Officer    25 Park Lane, London       1
Project Manager            20 Euston Road, London     2
Project Coordinator        12 Abbey Road, London      3
Designer                   5 Carnaby Street, London   4
========================= ========================== ===========

The resulting hierarchy table is a typical SQL table which can be used like any other table. For
example we can create a left-outer join to get all employees of the organization, including the ones
with no registered address:

.. code-block:: sql

  SELECT A.name, B.address, A.level
  FROM
  SELECT name, LEVEL(node) AS level FROM HIERARCHY (
  USING h_src AS v
  JOIN PRIOR u ON v.pred = u.succ
  ORDER SIBLINGS BY ord ASC
  START WHERE pred IS NULL
  SET node)
  AS H) A LEFT OUTER JOIN t_src B
  ON A.name = B.name


And the result would be:

========================= ========================== ===========
 name                      address                    level
========================= ========================== ===========
Chief Executive Officer    25 Park Lane, London        1
Project Manager            20 Euston Road, London      2
Sales Manager              null                        2
Project Coordinator        12 Abbey Road, London       3
Architect                  null                        3
Programmer                 null                        4
Designer                   5 Carnaby Street, London    4
========================= ========================== ===========

Using Hierarchies with Views
----------------------------

By using views it becomes much easier to work with hierarchies, especially when doing self-joins
to calculate binary predicates such :code:`IS_PARENT` and :code:`IS_FOLLOWING`.

Let us assume we have the :code:`h_src` table above. To create a view of a hierarchy that uses this
table we issue the following SQL statement:

.. code-block:: sql

  CREATE VIEW HV AS SELECT * FROM HIERARCHY (
  USING h_src AS v
  JOIN PRIOR u ON v.pred = u.succ
  ORDER SIBLINGS BY ord ASC
  START WHERE pred IS NULL
  SET Node)

The above command will create a view named :code:`HV` that wraps a hierarchy. From now on the view
name can be used in a SQL query and it will be replaced with the underlying hierarchy statement.

In the following example we join the hierarchy view to itself in order to get the names of the
children of the root.

.. code-block:: sql

  SELECT Children.name
  FROM HV Children, HV Parents
  WHERE IS_ROOT(Parents.Node) AND IS_PARENT(Parents.Node, Children.Node)


To select the addresses of all the descendants of the 2nd levels employees we need
to do two level join:
- The inner join will calculate the descendants of the 2nd level
employees.
- The outer join will join the result with the addresses table and gather
the names and the addresses:

.. code-block:: sql

  SELECT Emp.name, Addresses.address
  FROM
    (SELECT Descendants.name AS name
     FROM HV Parents, HV Descendants
     WHERE IS_DESCENDANT(Descendants.Node, Parents.Node) AND LEVEL(Parents.Node) = 2
    ) Emp,
    Addresses
  WHERE Emp.name = Addresses.name

Footnotes
---------

.. [#matchers] Currently we only support the :code:`PATH` matcher, in future we will support complex ones.
.. [#node] The node column is an internal column that is aimed to be used only within hierarchy UDFs. Trying to describe this column for example will result in :code:`<INTERNAL>` data type.