package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.*;

public class SparkQueryTaskNode extends AbstractTaskNode
{
    private String query;
    private SparkTranslator translator;
    
    public SparkQueryTaskNode(final SparkTranslator translator, final String query) {
        this.query = query;
        this.translator = translator;
    }
    
    public String getQuery() {
        return this.query;
    }
    
    public SparkTranslator getTranslator() {
        return this.translator;
    }
}
