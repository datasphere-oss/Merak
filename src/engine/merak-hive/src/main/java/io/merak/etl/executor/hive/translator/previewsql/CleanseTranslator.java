package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.*;

public class CleanseTranslator extends NodeSqlTranslator
{
    private final CleanseNode node;
    private boolean cleaningColumns;
    
    public CleanseTranslator(final CleanseNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    @Override
    protected boolean canUseStarInSelect(final PipelineNode node) {
        return !this.cleaningColumns && super.canUseStarInSelect(node);
    }
    
    public TaskNode translate() {
        this.cleaningColumns = true;
        String parentNodeName = this.getParent((PipelineNode)this.node);
        int i = 0;
        for (final CleanseNode.Transformation t : this.node.getTransformations()) {
            final String newNodeName = String.format("%s_t%d", this.node.getName(), i);
            if (!(t instanceof CleanseNode.LookupTransformation)) {
                throw new UnsupportedOperationException("Unknown transformation " + t);
            }
            this.translate(parentNodeName, newNodeName, (CleanseNode.LookupTransformation)t);
            ++i;
            parentNodeName = newNodeName;
        }
        this.cleaningColumns = false;
        this.start(this.node.getName()).select((PipelineNode)this.node).from(parentNodeName).end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
    
    private void translate(final String parentNodeName, final String newNodeName, final CleanseNode.LookupTransformation transformation) {
        this.start(newNodeName);
        this.select((PipelineNode)this.node, c -> {
            if (c.getMapping().getLinkedEntityId().equals(transformation.getCleanupColumn().getId())) {
                return String.format("`%s`.`%s` AS `%s`", transformation.getLookupTableName(), transformation.getValueColumnName(), c.getGivenName());
            }
            else {
                return String.format("`%s`.`%s` AS `%s`", c.getReferencedEntity().getReferencedEntity().getOwnerNode().getName(), c.getReferencedEntity().getGivenName(), c.getGivenName());
            }
        });
        this.from(parentNodeName);
        this.builder.append(String.format(" LEFT OUTER JOIN `%s`.`%s` ON (`%s`.`%s` = `%s`.`%s`)", transformation.getLookupSchemaName(), transformation.getLookupTableName(), transformation.getLookupTableName(), transformation.getLookupColumnName(), parentNodeName, transformation.getCleanupColumn().getGivenName()));
        this.end();
    }
}
