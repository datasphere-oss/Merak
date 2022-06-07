package io.merak.etl.executor.spark.executor;

import io.merak.etl.sdk.pipeline.*;
import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import com.google.common.base.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.dto.*;
import merak.tools.ExceptionHandling.*;

public class SparkInteractiveValidateExecutor implements TaskExecutor
{
    protected final Logger LOGGER;
    SparkValidatorTaskNode taskNode;
    EndVertex sink;
    SparkTranslatorState sparkTranslatorState;
    
    public SparkInteractiveValidateExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    public void execute(final Task task, final TaskExecutorContext ctx) {
        try {
            final RequestContext reqCtx = (RequestContext)ctx.getContext();
            this.LOGGER.trace("entering SparkInteractiveValidateExecutors");
            this.taskNode = (SparkValidatorTaskNode)task;
            (this.sparkTranslatorState = this.taskNode.getTranslatorState()).runTranslations();
            this.sink = this.taskNode.getSink();
            final DataFrameObject dataFrame = this.sparkTranslatorState.getDataFrame((PipelineNode)this.sink);
            dataFrame.getDataset();
        }
        catch (Exception e) {
            this.LOGGER.error("Error while trying to execute spark interactive validate query : {}", (Object)Throwables.getStackTraceAsString((Throwable)e));
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "INTERACTIVE_SPARK_VALIDATE_EXECUTION_ERROR");
        }
    }
}
