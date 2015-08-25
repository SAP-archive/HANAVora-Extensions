package org.apache.spark.sql;

import org.apache.spark.SparkContext;

/**
 * This singleton allows the velocity sql context to be accessed from the SPARK interpreter
 * in Zeppelin. Always use this and do not instantiate it directly
 *
 * How to use that: VelocitySqlContextProvider.getProvider().getSqlContext(sc)
 *
 */
public class VelocitySqlContextProvider {

    private static VelocitySQLContext instance = null;

    public synchronized VelocitySQLContext getSqlContext(SparkContext sc) {
        if(VelocitySqlContextProvider.instance == null) {
            VelocitySqlContextProvider.instance = new VelocitySQLContext(sc);
        }
        return VelocitySqlContextProvider.instance;
    }

    /**
     * Convenience method for getting statically the instance
     *
     * @return
     */
    public static synchronized VelocitySqlContextProvider getProvider(){
        return new VelocitySqlContextProvider();
    }
}
