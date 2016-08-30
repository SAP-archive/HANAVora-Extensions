package org.apache.spark.sql;

import org.apache.spark.SparkContext;

/**
 * The SapSQLIntepreter requires a SAP Sql Context provider for getting the SqlContext
 * Singleton, since this has to be mocked for the tests here is the appropriate mock
 *
 */
public class SapSqlContextProviderMock extends SapSqlContextProvider {

    private SQLContext vsc;

    public SapSqlContextProviderMock(SQLContext vsc){
        this.vsc = vsc;
    }

    @Override
    public synchronized SQLContext getSqlContext(SparkContext sc) {
        return this.vsc;
    }

}
