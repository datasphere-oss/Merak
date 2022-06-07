package io.merak.etl.executor.spark.executor;

import io.merak.etl.sdk.pipeline.*;

import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.executor.*;
import com.google.common.base.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import merak.tools.ExceptionHandling.*;
import io.merak.etl.executor.impl.*;

public class SparkInteractiveSelectExecutor implements TaskExecutor
{
    protected final Logger LOGGER;
    SparkSelectTableTaskNode taskNode;
    EndVertex targetNode;
    SparkTranslatorState sparkTranslatorState;
    
    public SparkInteractiveSelectExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        try {
            final RequestContext reqCtx = (RequestContext)ctx.getContext();
            this.taskNode = (SparkSelectTableTaskNode)task;
            this.targetNode = this.taskNode.getSink();
            (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
            final ExecutionResult result = SparkHelper.generateResult(this.sparkTranslatorState, (PipelineNode)this.targetNode, reqCtx.getLimit());
            ctx.set(task.getOutputKey(), (TaskOutput)result);
        }
        catch (Exception e) {
            this.LOGGER.error("Error while trying to execute spark interactive select query : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "INTERACTIVE_SPARK_SELECT_EXECUTION_ERROR");
        }
    }
}
