# HANA Vora Spark extensions

These are some extensions of *Apache Spark* developed for *SAP HANA Vora*. They can be used with
any supported Spark version, even without Vora. Note that some features might improve their
performance significantly if the HANA Vora datasource is used.

## First Steps

#### Prerequisites

Minimal requirements for the spark extensions:

  1. Java SE 7 (or later) installed
  2. Maven installed (`mvn -version` should work)
  3. Spark 1.6.1 installed (`SPARK_HOME` must be set to the installation directory)
     (Note that the only supported spark versions are 1.6.0 and 1.6.1!)

### Building

Build the distribution package with Maven:

```bash
mvn clean package
```

You can also skip the tests by adding the appropriate switch to the command line:

```bash
mvn clean package -D maven.test.skip
```

Then extract the package to its target directory:

```bash
export SAP_SPARK_HOME=$HOME/sap-spark-extensions # choose your install dir
mkdir -p $SAP_SPARK_HOME
tar xzpf ./dist/target/spark-sap-extensions-*-dist.tar.gz -C $SAP_SPARK_HOME
```

### Starting an Extended Spark Shell

From the command line, execute

```bash
$SAP_SPARK_HOME/bin/start-spark-shell.sh
```

### Using the Extensions

While the spark shell starts up with `SparkContext` and `SQLContext` predefined, you need to 
instantiate a `SapSQLContext` to make use of the extensions.

```scala
import org.apache.spark.sql._

val voraContext = new SapSQLContext(sc)

// now we can start with some SQL queries
voraContext.sql("SHOW TABLES").show()
```

## Further Documentation

### Package Documentation

*TODO*

### Inline Documentation

You can also start at the [rootdoc](core/src/main/rootdoc.txt) and work your way through the code
 from there.

## Troubleshooting

##### Wrong Spark Version

```stacktrace
java.lang.NoSuchMethodError: org.apache.spark.sql.catalyst.optimizer.Optimizer: method <init>()V not found
  at org.apache.spark.sql.extension.ExtendableOptimizer.<init>(ExtendableOptimizer.scala:13)
  at org.apache.spark.sql.hive.ExtendableHiveContext.optimizer$lzycompute(ExtendableHiveContext.scala:93)
  at org.apache.spark.sql.hive.ExtendableHiveContext.optimizer(ExtendableHiveContext.scala:92)
  at org.apache.spark.sql.execution.QueryExecution.optimizedPlan$lzycompute(QueryExecution.scala:43)
  at org.apache.spark.sql.execution.QueryExecution.optimizedPlan(QueryExecution.scala:43)
  ...
```

This happens when spark version 1.6.2 is used, due to a change in the `Optimizer` interface. 
Because of this compatibility issue, we only support versions 1.6.0 and 1.6.1.

