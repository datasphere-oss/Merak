package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.translator.*;

public class SparkSnowFlakeTaskNode extends AbstractTaskNode implements LoadTargetTaskNode
{
    private SnowFlakeTargetTranslator translator;
    private SparkTranslatorState sparkTranslatorState;
    private SnowFlakeTargetNode node;
    
    public SparkSnowFlakeTaskNode(final SnowFlakeTargetTranslator translator, final SnowFlakeTargetNode node, final SparkTranslatorState sparkTranslatorState) {
        this.translator = translator;
        this.sparkTranslatorState = sparkTranslatorState;
        this.node = node;
    }
    
    public LoadTarget getSink() {
        return (LoadTarget)this.node;
    }
    
    public LoadTargetTranslator getSinkTranslator() {
        return (LoadTargetTranslator)this.translator;
    }
    
    public TranslatorState getTranslatorState() {
        return (TranslatorState)this.sparkTranslatorState;
    }
}
