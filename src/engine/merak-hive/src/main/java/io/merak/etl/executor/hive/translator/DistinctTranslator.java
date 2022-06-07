package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class DistinctTranslator extends SqlTranslator
{
    private final DistinctNode node;
    
    public DistinctTranslator(final DistinctNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    @Override
    protected SqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter) {
        return this.select(node, formatter, "SELECT DISTINCT ", node::getOutputEntitiesUsed);
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendTemporaryDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getId());
        this.select((PipelineNode)this.node).from(parent);
        this.appendPostActions((PipelineNode)this.node);
        this.end();
        return null;
    }
}
