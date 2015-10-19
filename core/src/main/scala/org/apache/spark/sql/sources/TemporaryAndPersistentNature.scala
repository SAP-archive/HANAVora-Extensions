package org.apache.spark.sql.sources

/**
 * In general SQL terms temporary tables go away when the context closes (i.e. user session)
 * and persistent ones stay. The difference in the spark sense is they do not disappear
 * after spark is shut down.
 *
 * Any trait extending this one will indicate the temporary or persistent table creation must
 * be handled.
 */
private[sql] trait TemporaryAndPersistentNature
