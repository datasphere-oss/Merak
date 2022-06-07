package io.merak.etl.executor.hive.translator;

import java.util.*;
import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class JoinTranslator extends SqlTranslator
{
    private final JoinNode node;
    
    public JoinTranslator(final JoinNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    private String generateExpression(final JoinNode.RightPort rightTable) {
        if (rightTable.getMode().equals((Object)PipelineNode.Mode.ADVANCED)) {
            return rightTable.getExpression();
        }
        final StringBuilder builder = new StringBuilder();
        for (final JoinNode.RightPort.JoinColumn joinColumn : rightTable.getJoinColumns()) {
            builder.append(String.format("%s.%s = %s.%s AND ", this.node.getLeftTable().getId(), joinColumn.getLeftColumn().getInputName(), rightTable.getRightTable().getId(), joinColumn.getRightColumn().getInputName()));
        }
        builder.setLength(builder.length() - 5);
        return builder.toString();
    }
    
    public TaskNode translate() {
        final String nodeName = String.format("%s_l", this.node.getId());
        this.start(nodeName).select((PipelineNode)this.node, entity -> String.format("%s.%s AS %s", entity.getReferencedEntity().getOwnerNode().getId(), entity.getInputName(), entity.getOutputName()), i -> this.node.getInputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode())).from(this.node.getLeftTable().getId());
        for (final JoinNode.RightPort rightPort : this.node.getRightPorts()) {
            this.builder.append(String.format(" %s JOIN %s", rightPort.getJoinType(), rightPort.getRightTable().getId()));
            if (!rightPort.getJoinType().equalsIgnoreCase("CROSS")) {
                this.builder.append(String.format(" ON (%s)", this.generateExpression(rightPort)));
            }
        }
        this.end();
        final String parentName = this.appendDerivations((PipelineNode)this.node, nodeName);
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parentName);
        this.appendPostActions((PipelineNode)this.node);
        this.end();
        return null;
    }
}
