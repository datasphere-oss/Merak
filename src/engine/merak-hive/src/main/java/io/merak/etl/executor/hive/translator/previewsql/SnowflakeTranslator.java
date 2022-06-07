package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class SnowflakeTranslator extends NodeSqlTranslator
{
    private final SnowFlakeTargetNode node;
    
    public SnowflakeTranslator(final SnowFlakeTargetNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getName());
        this.select((PipelineNode)this.node).from(parent);
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
