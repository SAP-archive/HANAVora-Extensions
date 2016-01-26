package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.{UnresolvedException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.{InternalRow, trees}
import org.apache.spark.sql.types._

/**
  * Represents a node in the tree that filters the metadata of a [[NamedExpression]] underneath it.
 *
  * @param child The child.
  * @param filters The filtered annotations. Can be a '*' which means get all annotations.
  * @param exprId The expression id.
  *
  * TODO (YH, AC) either remove this class if we can implement table-valued functions in Spark or
  *               improve it and its usage in the parser.
  */
case class AnnotationFilter(child: Expression)(
  val filters: Set[String] = Set.empty,
  val exprId: ExprId = NamedExpression.newExprId)
  extends UnaryExpression
  with NamedExpression
  with CodegenFallback {

  override def name: String = child match {
    case e:NamedExpression => e.name
    case _ => throw new UnresolvedException(this, "name of AnnotationFilter with non-named child")
  }

  override lazy val resolved = childrenResolved

  override def toAttribute: Attribute = {
    if (resolved) {
      child.transform ({
        case a:Alias => a.copy(a.child, a.name)(a.exprId, qualifiers = a.qualifiers,
          explicitMetadata = Some(MetadataAccessor.filterMetadata(a.metadata, filters)))
        case a:AttributeReference =>
          a.copy(a.name, a.dataType, a.nullable,
            metadata = MetadataAccessor.filterMetadata(a.metadata, filters))(a.exprId, a.qualifiers)
        case p => p
      }) match {
        case e: NamedExpression => e.toAttribute
        case _ => throw new UnresolvedException(this, "toAttribute of AnnotationFilter with " +
          "no-named child")
      }
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case aa: AnnotationFilter => child == aa.child && filters == aa.filters &&
      exprId == aa.exprId
    case _ => false
  }

  // scalastyle:off magic.number
  override def hashCode:Int = {
    List[Int](child.hashCode, filters.hashCode, exprId.hashCode)
      .foldLeft(17)((l, r) => 31 * l + r)
  }

  override def metadata: Metadata = {
    child match {
      case named: NamedExpression => MetadataAccessor.filterMetadata(named.metadata, filters)
      case _ => Metadata.empty
    }
  }

  override def qualifiers: Seq[String] = Nil

  override def eval(input: InternalRow): Any = child.eval(input)

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override protected final def otherCopyArgs: Seq[AnyRef] = filters :: exprId :: Nil
}
