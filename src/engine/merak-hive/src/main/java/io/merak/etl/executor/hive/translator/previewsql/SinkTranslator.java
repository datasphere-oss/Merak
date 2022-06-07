package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.constants.*;

public abstract class SinkTranslator extends NodeSqlTranslator
{
    public SinkTranslator(final TranslatorContext translatorContext) {
        super(translatorContext);
    }
    
    @Override
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return false;
    }
    
    public TaskNode endTranslation(final PipelineNode node) {
        this.with().selectAll().from(node.getName());
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            this.explain();
            return (TaskNode)new QueryTaskNode(this.builder.toString());
        }
        if (this.requestContext.getProcessingContext().getBuildMode() == AwbConstants.BuildMode.INTERACTIVE) {
            this.limit();
            return (TaskNode)new InteractiveSelectQueryTaskNode(this.builder.toString(), (SinkVertex)node);
        }
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
    
    protected SinkTranslator with() {
        this.builder.insert(0, "WITH\n");
        this.builder.setLength(this.builder.length() - 2);
        this.builder.append("\n");
        return this;
    }
    
    protected SinkTranslator explain() {
        this.builder.insert(0, "EXPLAIN\n");
        return this;
    }
    
    protected SinkTranslator limit() {
        return this.limit(this.getSampleViewLimit());
    }
    
    protected SinkTranslator limit(final long limit) {
        this.builder.append(" LIMIT ").append(limit);
        return this;
    }
    
    protected SinkTranslator selectAll() {
        this.builder.append("SELECT *");
        return this;
    }
}
