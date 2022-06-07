package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

public class UnionTranslator extends SqlTranslator
{
    private final UnionNode node;
    
    public UnionTranslator(final UnionNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected SqlTranslator select(final PipelineNode unionNode) {
        final Entity inputEntity;
        return this.select((PipelineNode)this.node, c -> {
            inputEntity = this.node.getUnionNodeInputEntity(unionNode.getId(), c);
            return String.format("%s AS %s", inputEntity.getInputName(), c.getInputName());
        }, this.node::getOutputEntitiesUsed).from(unionNode.getId());
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return false;
    }
    
    public TaskNode translate() {
        final String union = String.format(" UNION %s ", AwbConfigs.useHiveUnionAll(this.requestContext) ? "ALL" : this.node.getUnionType().name());
        final String parentName = String.format("%s%s", this.node.getId(), AwbConfigs.useHiveUnionAll(this.requestContext) ? "_t1" : "");
        this.start(parentName);
        this.select(this.node.getUnionNodes().get(0));
        for (int i = 1; i < this.node.getUnionNodes().size(); ++i) {
            this.builder.append(union);
            this.select(this.node.getUnionNodes().get(i));
        }
        this.end();
        if (AwbConfigs.useHiveUnionAll(this.requestContext)) {
            this.start(this.node.getId());
            this.select((PipelineNode)this.node, x$0 -> super.defaultSelectFormatter(x$0), "SELECT DISTINCT ", this.node::getOutputEntitiesUsed);
            this.from(parentName);
            this.end();
        }
        return null;
    }
}
