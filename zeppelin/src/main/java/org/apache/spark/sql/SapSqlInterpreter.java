package org.apache.spark.sql;

import org.apache.commons.io.IOUtils;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.apache.zeppelin.spark.SparkInterpreter;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.DAGScheduler;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.sql.SQLContext.QueryExecution;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.ui.jobs.JobProgressListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.mutable.HashMap;
import scala.collection.mutable.HashSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * SAP SQL interpreter for Zeppelin.
 */
public class SapSqlInterpreter extends Interpreter {

    Logger logger = LoggerFactory.getLogger(SapSqlInterpreter.class);

    private SapSqlContextProvider vsqlProvider = SapSqlContextProvider.getProvider();

    private static final String TREEVIEWKEYWORD = "treeview";

    public static final String ERROR_PREFIX = "Error: ";
    public static final String FALLBACK_ERROR_MSG =
            "Please consult interpreter log file for details";

    /**
     * Constructor just for testing
     *
     * @param vsqlProvider a SapSQLContext provider
     */
    public SapSqlInterpreter(SapSqlContextProvider vsqlProvider) {
        super(new Properties());
        this.vsqlProvider = vsqlProvider;
    }

    static {
        Interpreter.register(
                "vora",
                "spark",
                SapSqlInterpreter.class.getName(),
                new InterpreterPropertyBuilder()
                        .add("zeppelin.spark.maxResult", "10000", "Max number of SparkSQL result to display.")
                        .add("zeppelin.spark.concurrentSQL", "false",
                                "Execute multiple SQL concurrently if set true.")
                        .build());
    }

    private String getJobGroup(InterpreterContext context) {
        return "zeppelin-" + this.hashCode() + "-" + context.getParagraphId();
    }

    private int maxResult;

    public SapSqlInterpreter(Properties property) {
        super(property);
    }

    @Override
    public void open() {
        this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));
    }

    private SparkInterpreter getSparkInterpreter() {
        for (Interpreter intp: getInterpreterGroup()) {
            if (intp.getClassName().equals(SparkInterpreter.class.getName())) {
                Interpreter p = intp;
                while (p instanceof WrappedInterpreter) {
                    if (p instanceof LazyOpenInterpreter) {
                        p.open();
                    }
                    p = ((WrappedInterpreter) p).getInnerInterpreter();
                }
                return (SparkInterpreter) p;
            }
        }
        throw new RuntimeException("SparkInterpreter not found!");
    }

    public boolean concurrentSQL() {
        return Boolean.parseBoolean(getProperty("zeppelin.spark.concurrentSQL"));
    }

    @Override
    public void close() {
    }


    @Override
    public InterpreterResult interpret(String st, InterpreterContext context) {

        SparkContext sc = getSparkInterpreter().getSparkContext();
        if (concurrentSQL()) {
            sc.setLocalProperty("spark.scheduler.pool", "fair");
        } else {
            sc.setLocalProperty("spark.scheduler.pool", null);
        }

        sc.setJobGroup(getJobGroup(context), "Zeppelin-Velocity", false);

        int viewType = 0;
        String outputParser = "%table ";
        String[] arr = st.split(" ", 5);
        String keyword = arr[0];
        String idColumn = null;
        String predColumn = null;
        String nameColumn = null;
        SapSQLContext vsqlc = null;

        if ( TREEVIEWKEYWORD.equalsIgnoreCase(keyword) ) {
            if (arr.length < 5) {
                String errMsg = "id column, pred column, name column can not be empty";
                logger.error(errMsg);
                return new InterpreterResult(Code.ERROR, formatErrorMessage(errMsg));
            }
            outputParser = "%angular ";
            idColumn = arr[1];
            predColumn = arr[2];
            if (idColumn.equalsIgnoreCase(predColumn)) {
                String errMsg = "id column, pred column can should be different";
                logger.error(errMsg);
                return new InterpreterResult(Code.ERROR, formatErrorMessage(errMsg));
            }
            nameColumn = arr[3];
            st = arr[4]; // actual sql command
            viewType = 1;
        }

        logger.info("Creating/Getting SapSQLContext...");
        vsqlc = this.vsqlProvider.getSqlContext(sc);

        // SchemaRDD - spark 1.1, 1.2, DataFrame - spark 1.3
        Object rdd;
        Object[] rows;
        try {
            rdd = vsqlc.sql(st);

            Method take = rdd.getClass().getMethod("take", int.class);
            rows = (Object[]) take.invoke(rdd, maxResult + 1);
        } catch (Exception e) {
            logger.error(e.toString());
            sc.clearJobGroup();
            return new InterpreterResult(Code.ERROR, parseFormattedMessageFromException(e));
        }

        String msg = null;

        // get field names
        Method queryExecution;
        QueryExecution qe;
        try {
            queryExecution = rdd.getClass().getMethod("queryExecution");
            qe = (QueryExecution) queryExecution.invoke(rdd);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            logger.error(e.toString());
            return new InterpreterResult(Code.ERROR, parseFormattedMessageFromException(e));
        }

        if (rows != null && rows.length > 0) {
            try {
                List<Attribute> columns =
                        scala.collection.JavaConverters.asJavaListConverter(
                                qe.analyzed().output()).asJava();
                // ArrayType, BinaryType, BooleanType, ByteType, DecimalType, DoubleType, DynamicType,
                // FloatType, FractionalType, IntegerType, IntegralType, LongType, MapType, NativeType,
                // NullType, NumericType, ShortType, StringType, StructType

                if (viewType == 0) {
                    for (Attribute col: columns) {
                        if (msg == null) {
                            msg = col.name();
                        } else {
                            msg += "\t" + col.name();
                        }
                    }

                    msg += "\n";
                    for (int r = 0; r < maxResult && r < rows.length; r++) {
                        Object row = rows[r];
                        Method isNullAt = row.getClass().getMethod("isNullAt", int.class);
                        Method apply = row.getClass().getMethod("apply", int.class);

                        for (int i = 0; i < columns.size(); i++) {
                            if (!(Boolean) isNullAt.invoke(row, i)) {
                                msg += apply.invoke(row, i).toString();
                            } else {
                                msg += "null";
                            }
                            if (i != columns.size() - 1) {
                                msg += "\t";
                            }
                        }
                        msg += "\n";
                    }
                } else {
                    msg = convertDataToTree(rows, columns, idColumn, predColumn, nameColumn);
                    logger.info("msg length: {}", (msg != null ? msg.length(): null));
                    msg = fillTreeviewScriptWithData(msg);
                }
            } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                    | IllegalArgumentException | InvocationTargetException e) {
                logger.error(e.toString());
                return new InterpreterResult(Code.ERROR, parseFormattedMessageFromException(e));
            }
        } else {
            outputParser = "%angular ";//parses html better
            msg = "\n<font color=blue>Command processed successfully with no results</font>";
        }

        if (rows.length > maxResult) {
            msg += "\n<font color=yellow>Results are limited by " + maxResult + ".</font>";
        }
        InterpreterResult rett = new InterpreterResult(Code.SUCCESS, outputParser + msg);
        sc.clearJobGroup();
        return rett;
    }

    @Override
    public void cancel(InterpreterContext context) {
        SQLContext sqlc = getSparkInterpreter().getSQLContext();
        SparkContext sc = sqlc.sparkContext();

        sc.cancelJobGroup(getJobGroup(context));
    }

    @Override
    public FormType getFormType() {
        return FormType.SIMPLE;
    }


    @Override
    public int getProgress(InterpreterContext context) {
        String jobGroup = getJobGroup(context);
        SQLContext sqlc = getSparkInterpreter().getSQLContext();
        SparkContext sc = sqlc.sparkContext();
        JobProgressListener sparkListener = getSparkInterpreter().getJobProgressListener();
        int completedTasks = 0;
        int totalTasks = 0;

        DAGScheduler scheduler = sc.dagScheduler();
        HashSet<ActiveJob> jobs = scheduler.activeJobs();
        Iterator<ActiveJob> it = jobs.iterator();
        while (it.hasNext()) {
            ActiveJob job = it.next();
            String g = (String) job.properties().get("spark.jobGroup.id");
            if (jobGroup.equals(g)) {
                int[] progressInfo = getProgressFromStage_1_1x(sparkListener, job.finalStage());
                totalTasks += progressInfo[0];
                completedTasks += progressInfo[1];
            }
        }

        if (totalTasks == 0) {
            return 0;
        }
        return completedTasks * 100 / totalTasks;
    }

    private int[] getProgressFromStage_1_1x(JobProgressListener sparkListener, Stage stage) {
        int numTasks = stage.numTasks();
        int completedTasks = 0;

        try {
            Method stageIdToData = sparkListener.getClass().getMethod("stageIdToData");
            HashMap<Tuple2<Object, Object>, Object> stageIdData =
                    (HashMap<Tuple2<Object, Object>, Object>) stageIdToData.invoke(sparkListener);
            Class<?> stageUIDataClass =
                    this.getClass().forName("org.apache.spark.ui.jobs.UIData$StageUIData");

            Method numCompletedTasks = stageUIDataClass.getMethod("numCompleteTasks");

            Set<Tuple2<Object, Object>> keys =
                    JavaConverters.asJavaSetConverter(stageIdData.keySet()).asJava();
            for (Tuple2<Object, Object> k: keys) {
                if (stage.id() == (int) k._1()) {
                    Object uiData = stageIdData.get(k).get();
                    completedTasks += (int) numCompletedTasks.invoke(uiData);
                }
            }
        } catch (Exception e) {
            logger.error("Error on getting progress information", e);
        }

        List<Stage> parents = JavaConversions.asJavaList(stage.parents());
        if (parents != null) {
            for (Stage s: parents) {
                int[] p = getProgressFromStage_1_1x(sparkListener, s);
                numTasks += p[0];
                completedTasks += p[1];
            }
        }
        return new int[]{numTasks, completedTasks};
    }

    @Override
    public Scheduler getScheduler() {
        if (concurrentSQL()) {
            int maxConcurrency = 10;
            return SchedulerFactory.singleton().createOrGetParallelScheduler(
                    SapSqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
        } else {
            // getSparkInterpreter() calls open() inside.
            // That means if SparkInterpreter is not opened, it'll wait until SparkInterpreter open.
            // In this moment UI displays 'READY' or 'FINISHED' instead of 'PENDING' or 'RUNNING'.
            // It's because of scheduler is not created yet, and scheduler is created by this function.
            // Therefore, we can still use getSparkInterpreter() here, but it's better and safe
            // to getSparkInterpreter without opening it.
            for (Interpreter intp: getInterpreterGroup()) {
                if (intp.getClassName().equals(SparkInterpreter.class.getName())) {
                    Interpreter p = intp;
                    return p.getScheduler();
                }
            }
            throw new InterpreterException("Can't find SparkInterpreter");
        }
    }

    @Override
    public List<String> completion(String buf, int cursor) {
        return null;
    }

    private String convertDataToTree(Object[] rows, List<Attribute> columns,
                                     String idColumn, String predColumn, String nameColumn)
            throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Map<String, List<SqlInterpreterNode>> map = new java.util.HashMap<>();
        SqlInterpreterNode rootNode = null;
        for (int r = 0; r < maxResult && r < rows.length; r++) {
            Object row = rows[r];
            Method isNullAt = row.getClass().getMethod("isNullAt", int.class);
            Method apply = row.getClass().getMethod("apply", int.class);

            String id = null, pred = null, name = null;
            for (int i = 0; i < columns.size(); i++) {
                String value = null;
                if (!(Boolean) isNullAt.invoke(row, i)) {
                    value = apply.invoke(row, i).toString();
                } else {
                    value = "null";
                }

                if (columns.get(i).name().equals(idColumn)) {
                    id = value;
                } else if (columns.get(i).name().equals(predColumn)) {
                    pred = value;
                }

                if (columns.get(i).name().equals(nameColumn)) {
                    name = value;
                }
            }
            // pred null value case depends on type of the variable
            if (pred == null || pred.equalsIgnoreCase("")
                    || pred.equalsIgnoreCase("null") || pred.equalsIgnoreCase("0")) {
                logger.info("root node found: {}, {}", id, name);
                rootNode = new SqlInterpreterNode(id, name);
            }

            if (map.get(pred) == null) {
                List<SqlInterpreterNode> list = new LinkedList<>();
                list.add(new SqlInterpreterNode(id, name));
                map.put(pred, list);
            } else {
                map.get(pred).add(new SqlInterpreterNode(id, name));
            }
        }

        addChildrenToNode(rootNode, map);

        return rootNode.toString();
    }

    private void addChildrenToNode(SqlInterpreterNode node, Map<String, List<SqlInterpreterNode>> map) {
        List<SqlInterpreterNode> list = map.get(node.getId());
        if (list != null) {
            node.setChildren(list);
            for (int i = 0; i < node.getChildren().size(); i++) {
                addChildrenToNode(node.getChildren().get(i), map);
            }
        }
    }

    private String fillTreeviewScriptWithData(String data) {
        ClassLoader classLoader = getClass().getClassLoader();
        String script = null;

        InputStream in = classLoader.getResourceAsStream("treeview.html");
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            IOUtils.copy(in, out);
            script = new String(out.toByteArray());
            logger.info("script length: "+(script!=null? script.length(): null) );
            script = script.replaceAll("TREEID", UUID.randomUUID().toString());
            script = script.replaceFirst("TREEDATA", data);
        } catch (IOException e) {
            logger.error("Error on reading resource file", e);
        }

        return script;
    }

    private static String formatErrorMessage(String msg) {

        return "%angular <font color=red>" + ERROR_PREFIX + msg + "</font>";
    }

    private static String parseFormattedMessageFromException(Exception e) {
        Throwable ex = e;
        String errMsg = FALLBACK_ERROR_MSG;

        while(ex != null) {
            String msg = ex.getMessage();
            if(msg != null && msg != "") {
                errMsg = msg;
                break;
            }
            ex = ex.getCause();
        }

        return formatErrorMessage(errMsg);
    }
}
