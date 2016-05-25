package org.apache.spark.sql.catalyst.analysis.systables

/**
  * A provider of system tables. The provider has to be of one or more [[ProviderKind]]s.
  */
trait SystemTableProvider {
  this: ProviderKind =>
}

/**
  * A kind of [[SystemTableProvider]]. Can be [[LocalSpark]] or [[ProviderBound]].
  */
sealed trait ProviderKind {
  this: SystemTableProvider =>
}

/**
  * A provider of [[SystemTable]]s that target local spark.
  */
trait LocalSpark extends ProviderKind {
  this: SystemTableProvider =>

  /**
    * Create the [[SystemTable]] for local spark.
    * @return The [[SystemTable]] for local spark.
    */
  def create(): SystemTable
}

/**
  * A provider of [[SystemTable]]s that target datasource providers.
  */
trait ProviderBound extends ProviderKind {
  this: SystemTableProvider =>

  /**
    * Create the [[SystemTable]] for the targeted provider.
    * @param provider The provider that shall be targeted
    * @param options The options for the provider
    * @return The [[SystemTable]] for the targeted provider.
    */
  def create(provider: String, options: Map[String, String]): SystemTable
}
