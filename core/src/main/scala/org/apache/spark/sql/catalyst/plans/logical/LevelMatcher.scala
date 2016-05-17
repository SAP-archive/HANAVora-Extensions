package org.apache.spark.sql.catalyst.plans.logical

/**
  * Determines how to map input rows to level hierarchy paths.
  */
sealed trait LevelMatcher

/**
  * Matches input RDD rows as paths.
  */
case object MatchPath extends LevelMatcher

/**
  * Collapses identical names in the same level to one node.
  *
  * @todo not implemented, semantics are yet to be defined.
  */
case object MatchLevelName extends LevelMatcher

/**
  * Collapses identical names across levels to one node.
  *
  * @todo not implemented, semantics are yet to be defined.
  */
case object MatchName extends LevelMatcher
