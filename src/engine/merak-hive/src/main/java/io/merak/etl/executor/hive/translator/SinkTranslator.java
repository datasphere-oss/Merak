package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.constants.*;

public abstract class SinkTranslator extends SqlTranslator
{
    public SinkTranslator(final TranslatorContext translatorContext) {
        super(translatorContext);
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    public TaskNode endTranslation(final PipelineNode node) {
        this.with().selectAll().from(node.getId());
        if (this.requestContext.getProcessingContext().isValidationMode()) {
            this.explain();
        }
        else {
            if (this.requestContext.getProcessingContext().isSchemaMode()) {
                return this.createView(node);
            }
            if (this.requestContext.getProcessingContext().getBuildMode() == AwbConstants.BuildMode.INTERACTIVE) {
                this.limit();
                return (TaskNode)new InteractiveSelectQueryTaskNode(this.builder.toString(), (SinkVertex)node);
            }
        }
        return null;
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
    
    protected TaskNode createView(final PipelineNode node) {
        final String createOutputView = String.format("CREATE VIEW `%s`.`%s` AS", AwbConfigs.getAWBWorkspaceSchema(), node.getId());
        final String dropOutputView = String.format("DROP VIEW `%s`.`%s`", AwbConfigs.getAWBWorkspaceSchema(), node.getId());
        final String createViewQuery = String.format("%s \n%s", createOutputView, this.builder.toString());
        return (TaskNode)new HiveSchemaTaskNode(createViewQuery, dropOutputView, (EndVertex)node);
    }
}
