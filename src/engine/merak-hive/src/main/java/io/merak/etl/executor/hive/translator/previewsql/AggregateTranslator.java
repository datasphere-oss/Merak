package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.function.*;
import java.util.stream.*;

import com.google.common.collect.*;
import com.google.common.base.*;

import java.util.*;

public class AggregateTranslator extends NodeSqlTranslator
{
    private final AggregateNode node;
    
    public AggregateTranslator(final AggregateNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return false;
    }
    
    @Override
    protected void end() {
        if (this.node.getGroupBys() != null && this.node.getGroupBys().size() > 0) {
            final String expressions = (String)this.node.getGroupBys().stream().map(Derivation::getOriginalExpression).collect(Collectors.joining(", "));
            this.builder.append(" GROUP BY ").append(expressions);
        }
        this.appendPostActions((PipelineNode)this.node);
        super.end();
    }
    
    public TaskNode translate() {
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        if (this.node.getAggregates().size() > 0) {
            final String s;
            this.node.getAggregates().forEach(i -> s = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getOriginalExpression()));
        }
        if (this.node.getGroupBys().size() > 0) {
            final String s2;
            this.node.getGroupBys().forEach(i -> s2 = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getOriginalExpression()));
        }
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendTemporaryDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getName());
        final String expression;
        this.select((PipelineNode)this.node, column -> {
            expression = columnToExpressionMap.get(column);
            Preconditions.checkNotNull((Object)expression, "No expression found for column %s!", new Object[] { column.getGivenName() });
            return String.format("%s AS `%s`", expression, column.getGivenName());
        });
        this.from(parent);
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
