package io.merak.etl.executor.spark.executor;

import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.context.*;
import io.merak.etl.utils.schema.*;
import io.merak.etl.executor.spark.metadata.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.executor.impl.*;
import io.merak.etl.sdk.executor.*;
import com.google.common.base.*;
import io.merak.etl.utils.config.*;
import merak.tools.ExceptionHandling.*;
import io.merak.etl.schema.*;
import java.util.*;
import io.merak.etl.utils.column.*;
import org.slf4j.*;

public class SparkSchemaExecutor implements TaskExecutor
{
    private static final Logger LOGGER;
    SparkSchemaTaskNode taskNode;
    EndVertex sink;
    SparkTranslatorState sparkTranslatorState;
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        try {
            final RequestContext reqCtx = (RequestContext)ctx.getContext();
            this.taskNode = (SparkSchemaTaskNode)task;
            this.sink = this.taskNode.getSink();
            (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
            final BaseSchemaHandler schemaInstance = SchemaFactory.createSchemaInstance(reqCtx);
            final List<TableColumn> tableSchema = (List<TableColumn>)schemaInstance.getObjectSchema((Object)this.sparkTranslatorState.getDataFrame((PipelineNode)this.sink).getDataset());
            final ExecutionResult result = new ExecutionResult(this.sink);
            SparkMetaDataUtils.populateSchema(tableSchema, result);
            ctx.set(task.getOutputKey(), (TaskOutput)result);
        }
        catch (Exception e) {
            SparkSchemaExecutor.LOGGER.error("Error while trying to execute spark schema execution query : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            SparkSchemaExecutor.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "SPARK_SCHEMA_EXECUTION_ERROR");
        }
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkSchemaExecutor.class);
    }
}
