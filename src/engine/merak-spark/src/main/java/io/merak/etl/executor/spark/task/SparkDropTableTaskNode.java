package io.merak.etl.executor.spark.task;

import io.merak.etl.task.impl.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.pipeline.nodes.*;

public class SparkDropTableTaskNode extends AbstractTaskNode
{
    private DropTableNode node;
    private DropTableTranslator dropTableTranslator;
    
    public SparkDropTableTaskNode(final DropTableTranslator translator, final DropTableNode node) {
        this.node = node;
        this.dropTableTranslator = translator;
    }
    
    public DropTableNode getNode() {
        return this.node;
    }
    
    public void setNode(final DropTableNode node) {
        this.node = node;
    }
}
