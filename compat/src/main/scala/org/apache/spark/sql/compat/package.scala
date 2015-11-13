package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.PhysicalRDD

import scala.language.experimental.macros
import scala.reflect.ClassTag

package object compat {

  def physicalRDD[ROW](output: Seq[Attribute],
                       rdd: RDD[ROW],
                       extraInfo: String)(
                      implicit rowClassTag: ClassTag[ROW]
  ): PhysicalRDD = {
    val className = rowClassTag.runtimeClass.getSimpleName
    org.apache.spark.SPARK_VERSION match {
      case v if v startsWith "1.4" =>
        // PysicalRDD(output, rdd)
        if (!className.endsWith("Row")) {
          sys.error(s"PhysicalRDD takes RDD[Row], got RDD[$className]")
        }
        val apply = PhysicalRDD.getClass.getDeclaredMethods
          .find(_.getName == "apply")
          .get
        apply.invoke(PhysicalRDD, output, rdd).asInstanceOf[PhysicalRDD]
      case v if v startsWith "1.5" =>
        // PysicalRDD(output, rdd, extraInfo)
        if (className != "InternalRow") {
          sys.error(s"PhysicalRDD takes RDD[InternalRow], got RDD[$className]")
        }
        val apply = PhysicalRDD.getClass.getDeclaredMethods
          .find(_.getName == "apply")
          .get
        apply.invoke(PhysicalRDD, output, rdd, extraInfo).asInstanceOf[PhysicalRDD]
      case other =>
        sys.error(s"SPARK_VERSION $other is not supported")
    }
  }

}
