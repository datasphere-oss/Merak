package io.merak.etl.executor.hive.translator;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import com.google.common.collect.*;
import java.util.*;

public class InteractiveLoadTargetSqlTranslator extends LoadTargetHiveTranslator
{
    public InteractiveLoadTargetSqlTranslator(final LoadTarget node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
    }
    
    @Override
    protected SqlTranslator select() {
        final Map<String, String> expressions = (Map<String, String>)Maps.newLinkedHashMap();
        this.resolveAuditColumn(expressions);
        final String auditExpression;
        this.select((PipelineNode)this.node, c -> {
            auditExpression = expressions.get(c.getGivenName());
            if (auditExpression == null) {
                return this.defaultSelectFormatter(c);
            }
            else {
                return String.format("%s %s", auditExpression, c.getOutputName((PipelineNode)this.node));
            }
        }, this.node::getOutputEntitiesUsed);
        return this;
    }
    
    public TaskNode translate() {
        this.injectAuditColumns();
        this.with();
        this.selectAll().from(this.parentNodeName);
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            this.explain();
            return (TaskNode)new QueryTaskNode(this.builder.toString());
        }
        if (this.requestContext.getProcessingContext().isSchemaMode()) {
            return this.createView((PipelineNode)this.node);
        }
        this.limit();
        return (TaskNode)new LoadTargetInteractiveSelectQueryTaskNode(this.builder.toString(), this.node);
    }
}
