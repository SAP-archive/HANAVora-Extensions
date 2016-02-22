package org.apache.spark.sql.util

/** Utility functions for typed functions. */
object GenericUtil {
  /** Typed extensions to any object.
    *
    * @param a The given argument
    */
  implicit class RichGeneric[A](val a: A) {
    /** Matches with the given partial function and returns an option.
      *
      * @param pf PartialFunction used for matching
      * @tparam B Output of the [[PartialFunction]]
      * @return [[Some]][[B]] if the function was defined, [[None]] otherwise
      */
    def matchOptional[B](pf: PartialFunction[A, B]): Option[B] = pf.lift(a)
  }
}
