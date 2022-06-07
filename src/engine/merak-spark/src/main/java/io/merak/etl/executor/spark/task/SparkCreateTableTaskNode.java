package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.translator.*;
import io.merak.etl.sdk.pipeline.*;

public class SparkCreateTableTaskNode extends AbstractTaskNode implements SinkTaskNode
{
    InteractiveTargetTranslator targetTranslator;
    TargetNode targetNode;
    SparkTranslatorState sparkTranslatorState;
    
    public SparkCreateTableTaskNode(final InteractiveTargetTranslator targetTranslator, final TargetNode targetNode, final SparkTranslatorState state) {
        this.targetTranslator = targetTranslator;
        this.targetNode = targetNode;
        this.sparkTranslatorState = state;
    }
    
    public TargetNode getSink() {
        return this.targetNode;
    }
    
    public TargetTranslator getSinkTranslator() {
        return (TargetTranslator)this.targetTranslator;
    }
    
    public SparkTranslatorState getTranslatorState() {
        return this.sparkTranslatorState;
    }
}
