
from py4j.protocol import Py4JError

from pyspark.sql import SQLContext

__all__ = ["SapSQLContext"]

class SapSQLContext(SQLContext):
    """A variant of Spark SQL that integrates with SAP HANA Vora.

    :param sparkContext: The SparkContext to wrap.
    :param sapSQLContext: An optional JVM Scala SapSQLContext. If set, we do not instantiate a new
        :class:`SapSQLContext` in the JVM, instead we make all calls to this object.
    """

    def __init__(self, sparkContext, sapSQLContext=None):
        """Creates a new SapSQLContext.

                >>> from datetime import datetime
                >>> from pyspark_vora import *
                >>> allTypes = sc.parallelize([Row(i=1, s="string", d=1.0, l=1,
                ...     b=True, list=[1, 2, 3], dict={"s": 0}, row=Row(a=1),
                ...     time=datetime(2014, 8, 1, 14, 1, 5))])
                >>> df = allTypes.toDF()
                >>> df.registerTempTable("allTypes")
                >>> q = 'select i+1, d+1, not b, list[1], dict["s"], time, row.a from allTypes where b and i > 0'
                >>> result = sqlContext.sql(q).collect()
                >>> list(map(lambda x: tuple(x), result)) # Row.__repr__ is not compatible Spark 1.4/1.5
                [(2, 2.0, False, 2, 0, datetime.datetime(2014, 8, 1, 14, 1, 5), 1)]
                >>> df.map(lambda x: (x.i, str(x.s), x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
                [(1, 'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        SQLContext.__init__(self, sparkContext, sapSQLContext)
        self._scala_SapSQLContext = sapSQLContext

    @property
    def _ssql_ctx(self):
        try:
            if self._scala_SapSQLContext is None:
                self._scala_SapSQLContext = self._jvm.SapSQLContext(self._jsc.sc())
            return self._scala_SapSQLContext
        except Py4JError as e:
            raise Exception("SapSQLContext is not supported", e)
