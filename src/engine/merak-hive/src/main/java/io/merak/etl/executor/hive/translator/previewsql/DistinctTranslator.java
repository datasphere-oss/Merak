package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.function.*;

public class DistinctTranslator extends NodeSqlTranslator
{
    private final DistinctNode node;
    
    public DistinctTranslator(final DistinctNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return false;
    }
    
    @Override
    protected NodeSqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter) {
        return this.select(node, formatter, "SELECT DISTINCT ");
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendTemporaryDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getName());
        String oldName;
        this.select((PipelineNode)this.node, entity -> {
            oldName = entity.getGivenName();
            if (entity.getReferencedEntity() instanceof Derivation) {
                oldName = ((Derivation)entity.getReferencedEntity()).getOriginalExpression();
            }
            else if (entity.getReferencedEntity() != null) {
                oldName = entity.getReferencedEntity().getGivenName();
            }
            return String.format("`%s` AS `%s`", oldName, entity.getGivenName());
        });
        this.from(parent);
        this.appendPostActions((PipelineNode)this.node);
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
