package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.sources.{Collection, Graph, RelationKind, ViewKind, Table => TableKind}

/** A target of a drop operation. */
sealed trait DropTarget {

  /**
    * Checks if the given [[RelationKind]] may be a target of this.
    *
    * @param relationKind The [[RelationKind]] to test.
    * @return `true` if the given [[RelationKind]] is accepted as target for a drop operation,
    *         `false` otherwise.
    */
  def accepts(relationKind: RelationKind): Boolean

  /** The name of the target. */
  def targetName: String
}

/** A table as target of a drop operation. */
case object TableTarget extends DropTarget {

  /** @inheritdoc */
  override def accepts(relationKind: RelationKind): Boolean =
    relationKind == TableKind

  /** @inheritdoc */
  def targetName: String = "Table"
}

/** A view as target of a drop operation. */
case object ViewTarget extends DropTarget {

  /** @inheritdoc */
  override def accepts(relationKind: RelationKind): Boolean =
    relationKind.isInstanceOf[ViewKind]

  /** @inheritdoc */
  def targetName: String = "View"
}

/** A collection as target of a drop operation. */
case object CollectionTarget extends DropTarget {

  /** @inheritdoc */
  override def accepts(relationKind: RelationKind): Boolean =
    relationKind == Collection

  /** @inheritdoc */
  override def targetName: String = "Collection"
}

/** A graph as target of a drop operation. */
case object GraphTarget extends DropTarget {

  /** @inheritdoc */
  override def accepts(relationKind: RelationKind): Boolean =
  relationKind == Graph

  /** @inheritdoc */
  override def targetName: String = "Graph"
}
