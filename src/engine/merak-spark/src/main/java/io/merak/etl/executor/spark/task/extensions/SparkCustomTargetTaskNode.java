package io.merak.etl.executor.spark.task.extensions;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.sdk.pipeline.*;

public class SparkCustomTargetTaskNode extends AbstractTaskNode implements SinkTaskNode
{
    private CustomTargetTranslator translator;
    private SparkTranslatorState sparkTranslatorState;
    private CustomTargetNode node;
    
    public SparkCustomTargetTaskNode(final CustomTargetTranslator translator, final CustomTargetNode node, final SparkTranslatorState sparkTranslatorState) {
        this.translator = translator;
        this.sparkTranslatorState = sparkTranslatorState;
        this.node = node;
    }
    
    public SparkTranslatorState getTranslatorState() {
        return this.sparkTranslatorState;
    }
    
    public CustomTargetNode getSink() {
        return this.node;
    }
    
    public CustomTargetTranslator getSinkTranslator() {
        return this.translator;
    }
}
