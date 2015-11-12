package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.compat.ExpressionDescription
import org.apache.spark.sql.catalyst.expressions.mathfuncs._

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

package object compat {

  /**
    * Implicits providing compatibility methods to [[FunctionRegistry]].
    *
    * @param registry
    */
  implicit class FunctionRegistryCompatOps(registry: FunctionRegistry) {

    /**
      * Register all builtin functions.
      */
    def registerBuiltins(): Unit =
      expressions.foreach {
        case (name, (info, builder)) => registry.registerFunction(name, builder)
      }

    /**
      * Register an [[Expression]]. This is a convenience method to reduce boilerplate
      * of [[FunctionRegistry.registerFunction()]].
      * Backported from Spark 1.5.
      *
      * The same expression can be registered multiple time with different names.
      * So this can be used to provide aliases.
      *
      * @param name Name of the function to be registered.
      * @param tag [[ClassTag]] for the expression.
      * @tparam T Expression type.
      */
    def registerExpression[T <: Expression](name: String)
                                           (implicit tag: ClassTag[T]): Unit = {
      val (_, (_, builder)) = expression[T](name)
      registry.registerFunction(name, builder)
    }
  }

  type FunctionBuilder = Seq[Expression] => Expression

  /**
    * A list of all builtin functions. This includes both builtin functions
    * from Spark 1.4, as well as backported functions from Spark 1.5.
    */
  private val expressions: Map[String, (ExpressionInfo, FunctionBuilder)] = Map(
    // misc non-aggregate functions
    expression[Abs]("abs"),
    expression[CreateArray]("array"),
    expression[Coalesce]("coalesce"),
    expression[Explode]("explode"),
    /* TODO: expression[Greatest]("greatest"), */
    expression[If]("if"),
    /* TODO: expression[IsNaN]("isnan"), */
    expression[IsNull]("isnull"),
    expression[IsNotNull]("isnotnull"),
    /* TODO: expression[Least]("least"), */
    expression[Coalesce]("nvl"),
    expression[Rand]("rand"),
    expression[Randn]("randn"),
    expression[CreateStruct]("struct"),
    /* TODO: expression[CreateNamedStruct]("named_struct"), */
    expression[Sqrt]("sqrt"),
    /* TODO: expression[NaNvl]("nanvl"), */

    // math functions
    expression[Acos]("acos"),
    expression[Asin]("asin"),
    expression[Atan]("atan"),
    expression[Atan2]("atan2"),
    /* XXX: expression[Bin]("bin"), */
    expression[Cbrt]("cbrt"),
    expression[Ceil]("ceil"),
    expression[Ceil]("ceiling"),
    expression[Cos]("cos"),
    expression[Cosh]("cosh"),
    /* XXX: expression[Conv]("conv"), */
    expression[EulerNumber]("e"),
    expression[Exp]("exp"),
    expression[Expm1]("expm1"),
    expression[Floor]("floor"),
    /* XXX: expression[Factorial]("factorial"), */
    expression[Hypot]("hypot"),
    /* XXX: expression[Hex]("hex"), */
    expression[Logarithm]("log"),
    expression[Log]("ln"),
    expression[Log10]("log10"),
    expression[Log1p]("log1p"),
    /* XXX: expression[Log2]("log2"), */
    expression[UnaryMinus]("negative"),
    expression[Pi]("pi"),
    expression[Pow]("pow"),
    expression[Pow]("power"),
    /* TODO: expression[Pmod]("pmod"), */
    /* TODO: expression[UnaryPositive]("positive"), */
    expression[Rint]("rint"),
    expression[Round]("round"),
    /* XXX: expression[ShiftLeft]("shiftleft"), */
    /* XXX: expression[ShiftRight]("shiftright"), */
    /* XXX: expression[ShiftRightUnsigned]("shiftrightunsigned"), */
    expression[Signum]("sign"),
    expression[Signum]("signum"),
    expression[Sin]("sin"),
    expression[Sinh]("sinh"),
    expression[Tan]("tan"),
    expression[Tanh]("tanh"),
    expression[ToDegrees]("degrees"),
    expression[ToRadians]("radians"),

    // aggregate functions
    expression[Average]("avg"),
    expression[Count]("count"),
    expression[First]("first"),
    expression[Last]("last"),
    expression[Max]("max"),
    expression[Min]("min"),
    expression[Sum]("sum"),

    // string functions
    /* XXX: expression[Ascii]("ascii"), */
    /* XXX: expression[Base64]("base64"), */
    expression[Concat]("concat"),
    /* TODO: expression[ConcatWs]("concat_ws"), */
    /* XXX: expression[Encode]("encode"), */
    /* XXX: expression[Decode]("decode"), */
    /* TODO: expression[FindInSet]("find_in_set"), */
    /* TODO: expression[FormatNumber]("format_number"), */
    /* TODO: expression[GetJsonObject]("get_json_object"), */
    /* XXX: expression[InitCap]("initcap"), */
    expression[Lower]("lcase"),
    expression[Lower]("lower"),
    expression[Length]("length"),
    /* XXX: expression[Levenshtein]("levenshtein"), */
    /* TODO: expression[RegExpExtract]("regexp_extract"), */
    /* TODO: expression[RegExpReplace]("regexp_replace"), */
    /* XXX: expression[StringInstr]("instr"), */
    expression[StringLocate]("locate"),
    expression[StringLPad]("lpad"),
    expression[StringTrimLeft]("ltrim"),
    /* TODO: expression[FormatString]("format_string"), */
    /* TODO: expression[FormatString]("printf"), */
    expression[StringRPad]("rpad"),
    /* XXX: expression[StringRepeat]("repeat"), */
    expression[StringReverse]("reverse"),
    expression[StringTrimRight]("rtrim"),
    /* XXX: expression[SoundEx]("soundex"), */
    /* XXX: expression[StringSpace]("space"), */
    /* TODO: expression[StringSplit]("split"), */
    expression[Substring]("substr"),
    expression[Substring]("substring"),
    /* TODO: expression[SubstringIndex]("substring_index"), */
    /* XXX: expression[StringTranslate]("translate"), */
    expression[StringTrim]("trim"),
    /* XXX: expression[UnBase64]("unbase64"), */
    expression[Upper]("ucase"),
    /* XXX: expression[Unhex]("unhex"), */
    expression[Upper]("upper"),

    // datetime functions
    expression[AddMonths]("add_months"),
    expression[CurrentDate]("current_date"),
    expression[CurrentTimestamp]("current_timestamp"),
    expression[DateDiff]("datediff"),
    expression[DateAdd]("date_add"),
    /* XXX: expression[DateFormatClass]("date_format"), */
    expression[DateSub]("date_sub"),
    expression[DayOfMonth]("day"),
    expression[DayOfYear]("dayofyear"),
    expression[DayOfMonth]("dayofmonth"),
    /* XXX: expression[FromUnixTime]("from_unixtime"), */
    /* XXX: expression[FromUTCTimestamp]("from_utc_timestamp"), */
    expression[Hour]("hour"),
    /* XXX: expression[LastDay]("last_day"), */
    expression[Minute]("minute"),
    expression[Month]("month"),
    /* XXX: expression[MonthsBetween]("months_between"), */
    /* XXX: expression[NextDay]("next_day"), */
    expression[Quarter]("quarter"),
    expression[Second]("second"),
    /* XXX: expression[ToDate]("to_date"), */
    /* XXX: expression[ToUTCTimestamp]("to_utc_timestamp"), */
    expression[TruncDate]("trunc"),
    /* XXX: expression[UnixTimestamp]("unix_timestamp"), */
    expression[WeekOfYear]("weekofyear"),
    expression[Year]("year")

    // collection functions
    /* TODO: expression[Size]("size"), */
    /* TODO: expression[SortArray]("sort_array"), */
    /* TODO: expression[ArrayContains]("array_contains"), */

    // misc functions
    /* TODO: expression[Crc32]("crc32"), */
    /* TODO: expression[Md5]("md5"), */
    /* TODO: expression[Sha1]("sha"), */
    /* TODO: expression[Sha1]("sha1"), */
    /* TODO: expression[Sha2]("sha2"), */
    /* TODO: expression[SparkPartitionID]("spark_partition_id"), */
    /* TODO: expression[InputFileName]("input_file_name") */
  )

  /** See usage above. Backported from Spark 1.5. */
  private def expression[T <: Expression](name: String)
                                         (implicit tag: ClassTag[T]):
  (String, (ExpressionInfo, FunctionBuilder)) = {

    // See if we can find a constructor that accepts Seq[Expression]
    val varargCtor = Try(tag.runtimeClass.getDeclaredConstructor(classOf[Seq[_]])).toOption
    val builder = (expressions: Seq[Expression]) => {
      if (varargCtor.isDefined) {
        // If there is an apply method that accepts Seq[Expression], use that one.
        Try(varargCtor.get.newInstance(expressions).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new AnalysisException(e.getMessage)
        }
      } else {
        // Otherwise, find an ctor method that matches the number of arguments, and use that.
        val params = Seq.fill(expressions.size)(classOf[Expression])
        val f = Try(tag.runtimeClass.getDeclaredConstructor(params: _*)) match {
          case Success(e) =>
            e
          case Failure(e) =>
            throw new AnalysisException(s"Invalid number of arguments for function $name")
        }
        Try(f.newInstance(expressions: _*).asInstanceOf[Expression]) match {
          case Success(e) => e
          case Failure(e) => throw new AnalysisException(e.getMessage)
        }
      }
    }

    val clazz = tag.runtimeClass
    val df = clazz.getAnnotation(classOf[ExpressionDescription])
    if (df != null) {
      (name,
        (new ExpressionInfo(clazz.getCanonicalName, name, df.usage(), df.extended()),
          builder))
    } else {
      (name, (new ExpressionInfo(clazz.getCanonicalName, name), builder))
    }
  }
}
