package io.merak.etl.executor.spark.sdk;

import io.merak.etl.sdk.execution.api.*;
import io.merak.etl.sdk.engine.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.context.*;
import io.merak.etl.context.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.executor.spark.executor.store.*;
import io.merak.etl.executor.spark.schema.*;
import io.merak.etl.executor.spark.session.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.sdk.schema.*;
import io.merak.etl.sdk.executor.*;

@ExecutionAPI(engine = BatchEngine.SPARK)
public class SparkExecutionHandler implements BaseHandler
{
    public TaskNode getSampleTask(final String pipelineId, final SourceVertex source) {
        return (TaskNode)new SparkCopySampleTaskNode(pipelineId, (SourceNode)source);
    }
    
    public TaskNode getSchemaSyncTask(final SinkVertex sinkNode, final TaskNode targetTaskNode) {
        final TargetNode targetNode = (TargetNode)sinkNode;
        if (!targetNode.isHiveCompatible()) {
            return (TaskNode)new SparkNonHiveSchemaSyncTaskNode(targetNode, targetTaskNode);
        }
        return (TaskNode)new SparkHiveCompatibleSchemaSyncTaskNode(targetNode, targetTaskNode);
    }
    
    public TranslatorState getTranslatorState(final Context context) {
        return (TranslatorState)new SparkTranslatorState((RequestContext)context);
    }
    
    public TranslatorStore getTranslatorStore() {
        return (TranslatorStore)new SparkTranslatorStore();
    }
    
    public ExecutionEngineSession getSession(final Context context) {
        return (ExecutionEngineSession)DFSparkSession.getInstance((RequestContext)context);
    }
    
    public SchemaHandler getSchemaHandler() {
        return (SchemaHandler)new SparkSchema();
    }
    
    public TaskExecutorStore getExecutorStore(final Context context) {
        return (TaskExecutorStore)new SparkTaskExecutorStore();
    }
    
    public String getPreviewQuery(final Context context) {
        throw new UnsupportedOperationException("Unsupported Action for Batch Engine Spark");
    }
}
