from py4j.protocol import Py4JError
from py4j.java_collections import MapConverter

from pyspark.rdd import RDD, _prepare_for_python_RDD
from pyspark.serializers import AutoBatchedSerializer, PickleSerializer
from pyspark.sql.types import Row, StringType, StructType, _verify_type, \
    _infer_schema, _has_nulltype, _merge_type, _create_converter, _python_to_sql_converter
from pyspark.sql import SQLContext


__all__ = ["SapSQLContext"]

class SapSQLContext(SQLContext):
    """A variant of Spark SQL that integrates with SAP VORA

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
                >>> sqlContext.sql('select i+1, d+1, not b, list[1], dict["s"], time, row.a '
                ...            'from allTypes where b and i > 0').collect()
                [Row(c0=2, c1=2.0, c2=False, c3=2, c4=0, time=datetime.datetime(2014, 8, 1, 14, 1, 5), a=1)]
                >>> df.map(lambda x: (x.i, str(x.s), x.d, x.l, x.b, x.time, x.row.a, x.list)).collect()
                [(1, 'string', 1.0, 1, True, datetime.datetime(2014, 8, 1, 14, 1, 5), 1, [1, 2, 3])]
        """
        SQLContext.__init__(self, sparkContext)
        if sapSQLContext:
            self._scala_SapSQLContext = sapSQLContext

    @property
    def _ssql_ctx(self):
        try:
            if not hasattr(self, '_scala_SapSQLContext'):
                self._scala_SapSQLContext = self._get_SapSQL_ctx()
            return self._scala_SapSQLContext
        except Py4JError as e:
            raise Exception("SapSQL Context is not supported", e)

    def _get_SapSQL_ctx(self):
        return self._jvm.SapSQLContext(self._jsc.sc())

