package org.apache.spark.sql;

import org.apache.spark.SparkContext;

/**
 * The SapSQLIntepreter requires a SAP Sql Context provider for getting the SqlContext
 * Singleton, since this has to be mocked for the tests here is the appropriate mock
 *
 */
public class SapSqlContextProviderMock extends SapSqlContextProvider {

    private SapSQLContext vsc;

    public SapSqlContextProviderMock(SapSQLContext vsc){
        this.vsc = vsc;
    }

    @Override
    public synchronized SapSQLContext getSqlContext(SparkContext sc) {
        return this.vsc;
    }

}
