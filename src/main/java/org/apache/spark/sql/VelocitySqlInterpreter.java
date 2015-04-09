package org.apache.spark.sql;

import com.nflabs.zeppelin.interpreter.*;
import com.nflabs.zeppelin.interpreter.InterpreterResult.Code;
import com.nflabs.zeppelin.scheduler.Scheduler;
import com.nflabs.zeppelin.scheduler.SchedulerFactory;
import com.nflabs.zeppelin.spark.SparkInterpreter;
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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Velocity SQL interpreter for Zeppelin.
 */
public class VelocitySqlInterpreter extends Interpreter {
    Logger logger = LoggerFactory.getLogger(VelocitySqlInterpreter.class);

    private VelocitySQLContext vsqlc;

    /**
     * Constructor just for testing
     *
     * @param vsqlc a VelocitySqlContext
     */
    public VelocitySqlInterpreter(VelocitySQLContext vsqlc) {
        super(new Properties());
        this.vsqlc = vsqlc;
    }

    static {
        Interpreter.register(
                "velo",
                "spark",
                VelocitySqlInterpreter.class.getName(),
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

    public VelocitySqlInterpreter(Properties property) {
        super(property);
    }

    @Override
    public void open() {
        this.maxResult = Integer.parseInt(getProperty("zeppelin.spark.maxResult"));
    }

    private SparkInterpreter getSparkInterpreter() {
        for (Interpreter intp : getInterpreterGroup()) {
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

        if (vsqlc == null) {
            logger.info("Creating VelocitySqlContext...");
            vsqlc = new VelocitySQLContext(sc);
        }

        // SchemaRDD - spark 1.1, 1.2, DataFrame - spark 1.3
        Object rdd;
        Object[] rows;
        try {
            rdd = vsqlc.sql(st);

            Method take = rdd.getClass().getMethod("take", int.class);
            rows = (Object[]) take.invoke(rdd, maxResult + 1);
        } catch (Exception e) {
            logger.error("Velocity error: ", e);
            sc.clearJobGroup();
            return new InterpreterResult(Code.ERROR, e.getMessage());
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
            throw new InterpreterException(e);
        }

        List<Attribute> columns =
                scala.collection.JavaConverters.asJavaListConverter(
                        qe.analyzed().output()).asJava();

        for (Attribute col : columns) {
            if (msg == null) {
                msg = col.name();
            } else {
                msg += "\t" + col.name();
            }
        }

        msg += "\n";

        // ArrayType, BinaryType, BooleanType, ByteType, DecimalType, DoubleType, DynamicType,
        // FloatType, FractionalType, IntegerType, IntegralType, LongType, MapType, NativeType,
        // NullType, NumericType, ShortType, StringType, StructType

        try {
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
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            throw new InterpreterException(e);
        }

        if (rows.length > maxResult) {
            msg += "\n<font color=red>Results are limited by " + maxResult + ".</font>";
        }
        InterpreterResult rett = new InterpreterResult(Code.SUCCESS, "%table " + msg);
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
            for (Tuple2<Object, Object> k : keys) {
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
            for (Stage s : parents) {
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
                    VelocitySqlInterpreter.class.getName() + this.hashCode(), maxConcurrency);
        } else {
            // getSparkInterpreter() calls open() inside.
            // That means if SparkInterpreter is not opened, it'll wait until SparkInterpreter open.
            // In this moment UI displays 'READY' or 'FINISHED' instead of 'PENDING' or 'RUNNING'.
            // It's because of scheduler is not created yet, and scheduler is created by this function.
            // Therefore, we can still use getSparkInterpreter() here, but it's better and safe
            // to getSparkInterpreter without opening it.
            for (Interpreter intp : getInterpreterGroup()) {
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
}
