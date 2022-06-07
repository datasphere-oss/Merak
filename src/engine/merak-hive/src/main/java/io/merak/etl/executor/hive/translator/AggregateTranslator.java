package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import com.google.common.base.*;
import java.util.*;

public class AggregateTranslator extends SqlTranslator
{
    private final AggregateNode node;
    
    public AggregateTranslator(final AggregateNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    @Override
    protected void end() {
        if (this.node.getGroupBys() != null && this.node.getGroupBys().size() > 0) {
            final String expressions = (String)this.node.getGroupBys().stream().map(Derivation::getExpression).collect(Collectors.joining(", "));
            this.builder.append(" GROUP BY ").append(expressions);
        }
        this.appendPostActions((PipelineNode)this.node);
        super.end();
    }
    
    public TaskNode translate() {
        final Map<Entity, String> columnToExpressionMap = (Map<Entity, String>)Maps.newHashMap();
        if (this.node.getAggregates().size() > 0) {
            final String s;
            this.node.getAggregates().forEach(i -> s = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getExpression()));
        }
        if (this.node.getGroupBys().size() > 0) {
            final String s2;
            this.node.getGroupBys().forEach(i -> s2 = columnToExpressionMap.put(i.getDerivedFields().get(0), i.getExpression()));
        }
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendTemporaryDerivations((PipelineNode)this.node, parent);
        final String expression;
        this.start(this.node.getId()).select((PipelineNode)this.node, column -> {
            expression = columnToExpressionMap.get(column);
            Preconditions.checkNotNull((Object)expression, "No expression found for column %s!", new Object[] { column.getGivenName() });
            return String.format("%s AS %s", expression, column.getOutputName((PipelineNode)this.node));
        }).from(parent);
        this.end();
        return null;
    }
}
