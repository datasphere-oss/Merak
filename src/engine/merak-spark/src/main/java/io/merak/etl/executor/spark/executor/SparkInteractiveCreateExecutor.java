package io.merak.etl.executor.spark.executor;

import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.base.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import merak.tools.ExceptionHandling.*;
import org.apache.spark.sql.*;

public class SparkInteractiveCreateExecutor implements TaskExecutor
{
    protected final Logger LOGGER;
    SparkCreateTableTaskNode taskNode;
    TargetNode targetNode;
    InteractiveTargetTranslator targetTranslator;
    SparkTranslatorState sparkTranslatorState;
    
    public SparkInteractiveCreateExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        try {
            final RequestContext reqCtx = (RequestContext)ctx.getContext();
            this.taskNode = (SparkCreateTableTaskNode)task;
            this.targetNode = this.taskNode.getSink();
            this.targetTranslator = (InteractiveTargetTranslator)this.taskNode.getSinkTranslator();
            (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
            final Dataset<Row> dataset = this.sparkTranslatorState.getDataFrame((PipelineNode)this.targetNode).getDataset();
            final DataFrameWriter dataFrameWriter = this.targetTranslator.getWriter(dataset);
            final String tableName = String.format("`%s`.`%s`", this.targetNode.getSchemaName(), this.targetNode.getTableName());
            this.LOGGER.trace("saving table {}", (Object)tableName);
            dataFrameWriter.mode(SaveMode.Overwrite).saveAsTable(tableName);
        }
        catch (Exception e) {
            this.LOGGER.error("Error while trying to execute spark interactive create query : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "INTERACTIVE_SPARK_CREATE_EXECUTION_ERROR");
        }
    }
}
