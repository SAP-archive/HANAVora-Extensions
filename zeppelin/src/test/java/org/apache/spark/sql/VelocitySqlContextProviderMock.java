package org.apache.spark.sql;

import org.apache.spark.SparkContext;

/**
 * The VelocitySqlIntepreter requires a Velocity Sql Context provider for getting the SqlContext
 * Singleton, since this has to be mocked for the tests here is the appropriate mock
 *
 */
public class VelocitySqlContextProviderMock extends VelocitySqlContextProvider {

    private VelocitySQLContext vsc;

    public VelocitySqlContextProviderMock(VelocitySQLContext vsc){
        this.vsc = vsc;
    }

    @Override
    public synchronized VelocitySQLContext getSqlContext(SparkContext sc) {
        return this.vsc;
    }

}
