import os
import doctest
from pyspark.context import SparkContext
from pyspark.sql import Row, SQLContext
from pyspark_vora.sql.context import SapSQLContext
import pyspark_vora.sql.context
import pyspark.sql.context

# tests for doctest
__test__ = {"context": SapSQLContext}

def _test():
    os.chdir(os.environ["SPARK_HOME"])

    globs = pyspark.sql.context.__dict__.copy()
    sc = SparkContext('local[4]', 'PythonTest')
    globs['sc'] = sc
    globs['sqlContext'] = SapSQLContext(sc)
    (failure_count, test_count) = doctest.testmod(
        pyspark_vora.sql.context, globs=globs, verbose=True,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
