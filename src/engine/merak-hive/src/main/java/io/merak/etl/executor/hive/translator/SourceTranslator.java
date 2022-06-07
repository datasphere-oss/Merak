package io.merak.etl.executor.hive.translator;

import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

public class SourceTranslator extends SqlTranslator
{
    protected final SourceNode node;
    private SourceTranslatorUtils sourceTranslatorUtils;
    
    public SourceTranslator(final SourceNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
        this.sourceTranslatorUtils = new SourceTranslatorUtils(node, translatorContext.getRequestContext());
    }
    
    @Override
    public SqlTranslator from(final String from) {
        SqlTranslator currentTranslator = this;
        if (!this.node.isView()) {
            currentTranslator = super.from(from);
        }
        else {
            final String viewQuery = this.sourceTranslatorUtils.getSourceViewQuery();
            this.builder.append(" FROM (");
            this.builder.append(viewQuery);
            this.builder.append(")");
            this.builder.append(String.format(" AS source_view_%s", this.node.getId()));
        }
        return currentTranslator;
    }
    
    @Override
    protected void end() {
        if (!this.node.isView() && this.node.loadIncrementally()) {
            this.builder.append(this.sourceTranslatorUtils.getIncrementalFilterCondition());
        }
        if (this.node.handleDeleteRecords() && !this.node.isInjected()) {
            if (this.node.getTableType().equalsIgnoreCase("source")) {
                final String columns = this.getColumnsFromStream(this::sourceSelectFormatter, "SELECT ", c -> this.node.getOutputEntitiesUsed(true).map(o -> o.getReferencedEntity()));
                this.builder.append(String.format(" UNION %s ", AwbConfigs.useHiveUnionAll(this.requestContext) ? "ALL" : ""));
                this.builder.append(this.sourceTranslatorUtils.constructDeletedRecordsQuery(columns));
                if (this.node.loadIncrementally()) {
                    this.builder.append(this.sourceTranslatorUtils.getIncrementalFilterCondition().replace(" WHERE ", " AND "));
                }
            }
            else {
                if (!this.node.loadIncrementally()) {
                    this.builder.append(" WHERE ");
                }
                else {
                    this.builder.append(" AND ");
                }
                this.builder.append(" `ZIW_IS_DELETED`='true' ");
            }
        }
        super.end();
    }
    
    public TaskNode translate() {
        final String parentNodeName = String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName());
        this.start(this.node.getId());
        this.select((PipelineNode)this.node, this::sourceSelectFormatter, "SELECT ", c -> this.node.getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).map(o -> o.getReferencedEntity())).from(parentNodeName).end();
        return null;
    }
}
