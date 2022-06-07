package io.merak.etl.executor.spark.translator;

import io.merak.etl.pipeline.nodes.*;
import io.merak.etl.translator.*;
import io.merak.etl.executor.spark.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;

public class DropTableTranslator extends SparkTranslator<DropTableNode>
{
    public DropTableTranslator(final DropTableNode node, final TranslatorContext translatorContext) {
        super((PipelineNode)node, translatorContext);
    }
    
    @Override
    public TaskNode translate() {
        return (TaskNode)new SparkDropTableTaskNode(this, (DropTableNode)this.node);
    }
    
    @Override
    protected void generateDataFrame() {
    }
}
