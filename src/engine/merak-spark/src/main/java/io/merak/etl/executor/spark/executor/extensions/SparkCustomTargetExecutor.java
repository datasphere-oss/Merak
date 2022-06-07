package io.merak.etl.executor.spark.executor.extensions;

import io.merak.etl.sdk.executor.*;
import io.merak.etl.extensions.api.spark.*;
import org.apache.spark.sql.*;
import org.slf4j.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.executor.spark.task.extensions.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.pipeline.dto.*;

public abstract class SparkCustomTargetExecutor implements TaskExecutor
{
    protected Logger LOGGER;
    protected CustomTargetNode customTargetNode;
    protected SparkCustomTarget target;
    protected Dataset datasetToWrite;
    protected SparkTranslatorState sparkTranslatorState;
    
    public SparkCustomTargetExecutor() {
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
    }
    
    protected void init(final Task task) {
        final SparkCustomTargetTaskNode taskNode = (SparkCustomTargetTaskNode)task;
        final CustomTargetTranslator translator = taskNode.getSinkTranslator();
        (this.sparkTranslatorState = taskNode.getTranslatorState()).runTranslations();
        this.target = translator.getCustomTarget();
        this.customTargetNode = translator.getCustomTargetNode();
        this.datasetToWrite = this.sparkTranslatorState.getDataFrame((PipelineNode)this.customTargetNode).getDataset();
    }
}
