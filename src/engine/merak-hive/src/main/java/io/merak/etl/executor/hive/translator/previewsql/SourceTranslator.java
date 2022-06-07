package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class SourceTranslator extends NodeSqlTranslator
{
    protected final SourceNode node;
    private SourceTranslatorUtils sourceTranslatorUtils;
    
    public SourceTranslator(final SourceNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
        this.sourceTranslatorUtils = new SourceTranslatorUtils(node, translatorContext.getRequestContext());
    }
    
    @Override
    protected NodeSqlTranslator from(final String from) {
        if (!this.node.isView()) {
            return super.from(from);
        }
        final String viewQuery = this.sourceTranslatorUtils.getSourceViewQuery();
        this.builder.append(" FROM (");
        this.builder.append(viewQuery);
        this.builder.append(")");
        this.builder.append(String.format(" AS source_view_%s", this.node.getName()));
        return this;
    }
    
    @Override
    protected void end() {
        if (!this.node.isView() && this.node.loadIncrementally()) {
            this.builder.append(this.sourceTranslatorUtils.getIncrementalFilterCondition());
        }
        super.end();
    }
    
    public TaskNode translate() {
        final String parentNodeName = String.format("%s`.`%s", this.node.getSchemaName(), this.node.getTableName());
        this.start(this.node.getName());
        this.select((PipelineNode)this.node);
        this.from(parentNodeName);
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
