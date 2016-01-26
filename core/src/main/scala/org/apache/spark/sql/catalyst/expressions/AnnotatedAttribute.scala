package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import collection.immutable.Map

/**
 * Represents an annotation of an attribute, it carries annotation information of the underlying
 * child node.
 *
 * @param child the child expression.
 * @param annotations the annotations of the child.
 */
// scalastyle:off equals.hash.code
case class AnnotatedAttribute(child: Expression)(
  val annotations: Map[String, Expression] = Map.empty,
  val exprId: ExprId = NamedExpression.newExprId)
  extends UnaryExpression
  with NamedExpression
  with CodegenFallback {

  override def name: String = s"Annotation$exprId"

  override lazy val resolved = childrenResolved && !child.isInstanceOf[Generator] &&
    !annotations.exists(!_._2.resolved)

  // TODO (YH) we should fine-tune this, AnnotatedAttribute must be created with NamedExpression.
  // Modifications to the parser are needed for this.
  override def toAttribute: Attribute = {
    if (resolved) {
      child match {
        case ne:NamedExpression => ne.toAttribute
        case _ => sys.error(s"Can not convert child '${child.simpleString}' to Attribute.")
      }
    } else {
      UnresolvedAttribute(name)
    }
  }

  override def equals(other: Any): Boolean = other match {
    case aa: AnnotatedAttribute => child == aa.child && annotations == aa.annotations &&
      exprId == aa.exprId
    case _ => false
  }

  // scalastyle:off magic.number
  override def hashCode:Int = {
    List[Int](child.hashCode, annotations.hashCode, exprId.hashCode)
      .foldLeft(17)((l, r) => 31 * l + r)
  }

  /**
    * @return The metadata after propagation from the underlying node.
    */
  override def metadata: Metadata = {
    child match {
      case n:NamedExpression => MetadataAccessor.propagateMetadata(n.metadata,
        MetadataAccessor.expressionMapToMetadata(annotations))
      case p => MetadataAccessor.expressionMapToMetadata(annotations)
    }
  }

  override def qualifiers: Seq[String] = Nil

  override def eval(input: InternalRow): Any = child.eval(input)

  override def nullable: Boolean = child.nullable

  override def dataType: DataType = child.dataType

  override protected final def otherCopyArgs: Seq[AnyRef] = annotations :: exprId :: Nil
}
