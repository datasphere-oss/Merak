package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class UnionTranslator extends NodeSqlTranslator
{
    private final UnionNode node;
    
    public UnionTranslator(final UnionNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected NodeSqlTranslator select(final PipelineNode unionNode) {
        final Entity inputEntity;
        return this.select((PipelineNode)this.node, c -> {
            inputEntity = this.node.getUnionNodeInputEntity(unionNode.getId(), c);
            return String.format("`%s` AS `%s`", inputEntity.getReferencedEntity().getGivenName(), c.getGivenName());
        }).from(unionNode.getName());
    }
    
    @Override
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return false;
    }
    
    public TaskNode translate() {
        this.start(this.node.getName());
        final String union = String.format(" UNION %s ", this.node.getUnionType().name());
        this.select(this.node.getUnionNodes().get(0));
        for (int i = 1; i < this.node.getUnionNodes().size(); ++i) {
            this.builder.append(union);
            this.select(this.node.getUnionNodes().get(i));
        }
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
