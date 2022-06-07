package io.merak.etl.executor.spark.executor.extensions;

import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.executor.*;
import io.merak.etl.context.*;
import merak.tools.ExceptionHandling.*;
import io.merak.etl.extensions.exceptions.*;
import com.google.common.collect.*;
import io.merak.etl.extensions.api.spark.*;
import java.util.*;

public class SparkCustomTargetBatchExecutor extends SparkCustomTargetExecutor
{
    public void execute(final Task task, final TaskExecutorContext ctx) {
        final RequestContext reqCtx = (RequestContext)ctx.getContext();
        this.init(task);
        if (reqCtx.getProcessingContext().isValidationMode()) {
            this.validate();
        }
        else {
            this.initialiseBatch();
            this.executeBatch();
        }
    }
    
    private void validate() {
        this.LOGGER.info("Validation mode for custom target {}", (Object)this.customTargetNode.getClassName());
        this.datasetToWrite.explain();
    }
    
    private void executeBatch() {
        this.LOGGER.info("Execution mode for custom target {}", (Object)this.customTargetNode.getClassName());
        try {
            this.LOGGER.info("Writing to custom target {} ", (Object)this.target.toString());
            this.target.writeToTarget(this.datasetToWrite);
            this.LOGGER.info("Wrote to custom target {} ", (Object)this.target.toString());
        }
        catch (ExtensionsProcessingException e) {
            final String error = String.format("Error while processing custom class %s", this.customTargetNode.getClassName());
            this.LOGGER.error(error, (Throwable)e);
            IWRunTimeException.propagate((Exception)e, "CUSTOM_TARGET_WRITE_ERROR");
        }
    }
    
    private void initialiseBatch() {
        this.LOGGER.info("Initializing custom target {}", (Object)this.target.toString());
        final Map<String, Object> processingContext = (Map<String, Object>)Maps.newHashMap();
        this.target.initialiseContext(this.sparkTranslatorState.getSpark(), this.customTargetNode.getUserProperties(), new ProcessingContext((Map)processingContext));
    }
}
