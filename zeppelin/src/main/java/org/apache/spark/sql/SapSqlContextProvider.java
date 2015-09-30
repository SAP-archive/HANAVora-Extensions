package org.apache.spark.sql;

import org.apache.spark.SparkContext;

/**
 * This singleton allows the SapSQLContext to be accessed from the SPARK interpreter
 * in Zeppelin. Always use this and do not instantiate it directly
 *
 * How to use that: SapSqlContextProvider.getProvider().getSqlContext(sc)
 *
 */
public class SapSqlContextProvider {

    private static SQLContext instance = null;

    public synchronized SQLContext getSqlContext(SparkContext sc) {
        if(SapSqlContextProvider.instance == null) {
            SapSqlContextProvider.instance = new SapSQLContext(sc);
        }
        return SapSqlContextProvider.instance;
    }

    /**
     * Convenience method for getting statically the instance
     *
     * @return
     */
    public static synchronized SapSqlContextProvider getProvider(){
        return new SapSqlContextProvider();
    }
}
