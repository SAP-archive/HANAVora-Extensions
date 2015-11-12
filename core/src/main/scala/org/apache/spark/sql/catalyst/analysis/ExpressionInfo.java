package org.apache.spark.sql.catalyst.analysis;

//
// Backported from Spark 1.5.
//
/**
 * Expression information, will be used to describe a expression.
 */
public class ExpressionInfo {
  private String className;
  private String usage;
  private String name;
  private String extended;

  public String getClassName() {
    return className;
  }

  public String getUsage() {
    return usage;
  }

  public String getName() {
    return name;
  }

  public String getExtended() {
    return extended;
  }

  public ExpressionInfo(String className, String name, String usage, String extended) {
    this.className = className;
    this.name = name;
    this.usage = usage;
    this.extended = extended;
  }

  public ExpressionInfo(String className, String name) {
    this(className, name, null, null);
  }
}
