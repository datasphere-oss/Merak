package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.*;

public class CleanseTranslator extends SqlTranslator
{
    private final CleanseNode node;
    private boolean cleaningColumns;
    
    public CleanseTranslator(final CleanseNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return !this.cleaningColumns && super.canUseStarInSelect(entitiesUsedFormatter);
    }
    
    public TaskNode translate() {
        this.cleaningColumns = true;
        String parentNodeName = this.getParent((PipelineNode)this.node);
        parentNodeName = this.appendDerivations((PipelineNode)this.node, parentNodeName);
        int i = 0;
        for (final CleanseNode.Transformation t : this.node.getTransformations()) {
            if (!this.requestContext.getProcessingContext().isValidationMode() && !this.node.isTransformationUsed(t)) {
                continue;
            }
            final String newNodeName = String.format("%s_t%d", this.node.getId(), i);
            if (!(t instanceof CleanseNode.LookupTransformation)) {
                throw new UnsupportedOperationException("Unknown transformation " + t);
            }
            this.translate(parentNodeName, newNodeName, (CleanseNode.LookupTransformation)t);
            ++i;
            parentNodeName = newNodeName;
        }
        this.cleaningColumns = false;
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parentNodeName).end();
        return null;
    }
    
    private void translate(final String parentNodeName, final String newNodeName, final CleanseNode.LookupTransformation transformation) {
        this.start(newNodeName);
        this.select((PipelineNode)this.node, c -> {
            if (c.getReferencedEntity().getId().equals(transformation.getCleanupColumn().getId())) {
                return String.format("`%s`.`%s` AS %s", transformation.getLookupTableName(), transformation.getValueColumnName(), c.getOutputName((PipelineNode)this.node));
            }
            else {
                return String.format("%s.%s", parentNodeName, c.getOutputName((PipelineNode)this.node));
            }
        }, c -> this.node.getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()));
        this.from(parentNodeName);
        this.builder.append(String.format(" LEFT OUTER JOIN `%s`.`%s` ON (`%s`.`%s` = %s.%s)", transformation.getLookupSchemaName(), transformation.getLookupTableName(), transformation.getLookupTableName(), transformation.getLookupColumnName(), parentNodeName, transformation.getCleanupColumn().getOutputName((PipelineNode)this.node)));
        this.end();
    }
}
