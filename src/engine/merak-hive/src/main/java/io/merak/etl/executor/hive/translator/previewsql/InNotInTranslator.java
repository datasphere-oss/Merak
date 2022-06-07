package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class InNotInTranslator extends NodeSqlTranslator
{
    private final InNotInNode node;
    
    public InNotInTranslator(final InNotInNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    private String getFormattedPredicate() {
        String predicate = this.node.getPredicate().trim();
        predicate = String.format("`%s`.`%s` %s", this.node.getOuterTable().getName(), this.node.getOuterEntity().getGivenName(), this.node.getType());
        predicate = predicate.concat(String.format(" (SELECT `%s` AS `%s` FROM `%s`)", this.node.getInnerEntity().getGivenName(), "ZIW_" + this.node.getInnerEntity().getGivenName(), this.node.getInnerTable().getName()));
        return predicate;
    }
    
    public TaskNode translate() {
        final String parent = this.node.getOuterTable().getName();
        final String predicate = this.getFormattedPredicate();
        this.start(this.node.getName());
        this.select((PipelineNode)this.node, entity -> String.format("`%s` AS `%s`", entity.getGivenName(), entity.getGivenName())).from(parent);
        this.builder.append(String.format(" WHERE %s", predicate));
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
