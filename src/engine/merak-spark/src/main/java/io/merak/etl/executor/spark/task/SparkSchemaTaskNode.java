package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.translator.*;

public class SparkSchemaTaskNode extends AbstractTaskNode implements SinkTaskNode
{
    private static final String RESULT_SET = "resultSet";
    SinkTranslator translator;
    EndVertex sink;
    SparkTranslatorState sparkTranslatorState;
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public SparkSchemaTaskNode(final SinkTranslator sinkTranslator, final EndVertex sink, final SparkTranslatorState state) {
        this.translator = sinkTranslator;
        this.sink = sink;
        this.sparkTranslatorState = state;
    }
    
    public EndVertex getSink() {
        return this.sink;
    }
    
    public SinkTranslator getSinkTranslator() {
        return this.translator;
    }
    
    public SparkTranslatorState getTranslatorState() {
        return this.sparkTranslatorState;
    }
}
