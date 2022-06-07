package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class MaprDBInteractiveTargetTranslator extends LoadTargetHiveTranslator<LoadTarget>
{
    public MaprDBInteractiveTargetTranslator(final LoadTarget node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.parentNodeName = this.getParent((PipelineNode)node);
    }
    
    public TaskNode translate() {
        this.injectAuditColumns();
        this.with();
        this.selectAll().from(this.parentNodeName);
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            this.explain();
            return (TaskNode)new QueryTaskNode(this.builder.toString());
        }
        this.limit();
        return (TaskNode)new LoadTargetInteractiveSelectQueryTaskNode(this.builder.toString(), this.node);
    }
}
