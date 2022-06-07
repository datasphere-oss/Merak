package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.translator.*;

public class SparkSelectTableTaskNode extends AbstractTaskNode implements SinkTaskNode
{
    private static final String RESULT_SET = "resultSet";
    SinkTranslator targetTranslator;
    EndVertex targetNode;
    SparkTranslatorState sparkTranslatorState;
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public SparkSelectTableTaskNode(final SinkTranslator targetTranslator, final EndVertex targetNode, final SparkTranslatorState state) {
        this.targetTranslator = targetTranslator;
        this.targetNode = targetNode;
        this.sparkTranslatorState = state;
    }
    
    public EndVertex getSink() {
        return this.targetNode;
    }
    
    public SinkTranslator getSinkTranslator() {
        return this.targetTranslator;
    }
    
    public SparkTranslatorState getTranslatorState() {
        return this.sparkTranslatorState;
    }
}
