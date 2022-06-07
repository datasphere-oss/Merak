package io.merak.etl.executor.spark.executor;

import org.apache.spark.sql.*;
import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.task.*;
import merak.tools.ExceptionHandling.*;

public class SparkQueryExecutor implements TaskExecutor
{
    private SparkQueryTaskNode taskNode;
    private SparkSession spark;
    private final Logger LOGGER;
    
    public SparkQueryExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)SparkQueryExecutor.class);
    }
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        this.taskNode = (SparkQueryTaskNode)task;
        this.spark = this.taskNode.getTranslator().getSpark();
        try {
            this.spark.sql(this.taskNode.getQuery());
        }
        catch (Exception e) {
            this.LOGGER.trace("error running query {}", (Object)this.taskNode.getQuery(), (Object)e);
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "SPARK_QUERY_EXECUTION_ERROR");
        }
    }
}
