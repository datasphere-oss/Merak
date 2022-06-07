package io.merak.etl.executor.hive.translator;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class DeriveTranslator extends SqlTranslator
{
    private final DeriveNode node;
    
    public DeriveTranslator(final DeriveNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parent);
        this.end();
        return null;
    }
}
