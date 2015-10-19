package org.apache.spark.sql.execution

object ProviderUtils {

  /**
   * Create an instance from a specific provider class.
   * @param provider package where the provider is
   * @param executedCommandName ID to generate a good error description
   * @return a provider instance
   */
  def instantiateProvider[T](provider: String, executedCommandName: String): T = {
    try {
      Class.forName(provider).newInstance().asInstanceOf[T]
    } catch {
      case cnf: ClassNotFoundException =>
        try {
          Class.forName(provider + ".DefaultSource").newInstance()
            .asInstanceOf[T]
        } catch {
          case e => throw
            new ClassNotFoundException(
              s"""Cannot instantiate $provider.DefaultSource to execute $executedCommandName""", e)
        }
      case e => throw new ClassNotFoundException(
        s"""Cannot instantiate $provider to execute $executedCommandName""", e)
    }
  }
}
