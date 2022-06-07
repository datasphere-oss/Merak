package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.*;
import java.util.stream.*;

public class JoinTranslator extends NodeSqlTranslator
{
    private final JoinNode node;
    
    public JoinTranslator(final JoinNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    private String generateExpression(final JoinNode.RightPort rightTable) {
        if (rightTable.getMode().equals((Object)PipelineNode.Mode.ADVANCED)) {
            return rightTable.getOriginalExpression();
        }
        final StringBuilder builder = new StringBuilder();
        for (final JoinNode.RightPort.JoinColumn joinColumn : rightTable.getJoinColumns()) {
            builder.append(String.format("`%s`.`%s` = `%s`.`%s` AND ", this.node.getLeftTable().getName(), joinColumn.getLeftColumn().getReferencedEntity().getGivenName(), rightTable.getRightTable().getName(), joinColumn.getRightColumn().getReferencedEntity().getGivenName()));
        }
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }
    
    public TaskNode translate() {
        String parent = String.format("%s_0", this.node.getName());
        this.start(parent);
        this.select((PipelineNode)this.node, entity -> {
            if (entity.getReferenceType().equals((Object)Entity.Mapping.REFERENCE_TYPE.INPUT)) {
                return String.format("`%s`.`%s` AS `%s`", entity.getReferencedEntity().getOwnerNode().getName(), entity.getReferencedEntity().getGivenName(), entity.getGivenName());
            }
            else if (entity.getReferenceType().equals((Object)Entity.Mapping.REFERENCE_TYPE.DERIVATION)) {
                return String.format("%s AS `%s`", ((Derivation)entity.getReferencedEntity()).getOriginalExpression(), entity.getGivenName());
            }
            else {
                return String.format("`%s`", entity.getGivenName());
            }
        }, () -> this.node.getInputEntities().stream());
        this.from(this.node.getLeftTable().getName());
        for (final JoinNode.RightPort rightPort : this.node.getRightPorts()) {
            this.builder.append(String.format(" %s JOIN `%s`", rightPort.getJoinType(), rightPort.getRightTable().getName()));
            if (!rightPort.getJoinType().equalsIgnoreCase("CROSS")) {
                this.builder.append(String.format(" ON (%s)", this.generateExpression(rightPort)));
            }
        }
        this.end();
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getName());
        this.select((PipelineNode)this.node).from(parent).appendPostActions((PipelineNode)this.node);
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
