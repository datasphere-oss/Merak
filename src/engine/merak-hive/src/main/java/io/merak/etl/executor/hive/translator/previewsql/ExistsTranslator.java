package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class ExistsTranslator extends NodeSqlTranslator
{
    private final ExistsNode node;
    
    public ExistsTranslator(final ExistsNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        this.start(this.node.getName());
        this.select((PipelineNode)this.node, entity -> String.format("`%s` AS `%s`", entity.getGivenName(), entity.getGivenName())).from(this.node.getOuterTable().getName());
        final String innerTableId = this.node.getInnerTable().getName();
        final String expression = this.node.getOriginalExpression();
        this.builder.append(String.format(" WHERE %s (SELECT 1 FROM `%s` WHERE %s)", this.node.getType(), innerTableId, expression));
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
