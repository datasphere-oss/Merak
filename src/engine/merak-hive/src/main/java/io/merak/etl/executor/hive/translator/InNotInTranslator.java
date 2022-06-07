package io.merak.etl.executor.hive.translator;

import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class InNotInTranslator extends SqlTranslator
{
    private final InNotInNode node;
    
    public InNotInTranslator(final InNotInNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    private String getFormattedPredicate() {
        String predicate = this.node.getPredicate().trim();
        predicate = String.format("%s.%s %s", this.node.getOuterTable().getId(), this.node.getOuterEntity().getInputName(), this.node.getType());
        predicate = predicate.concat(String.format(" (SELECT %s as %s FROM %s)", this.node.getInnerEntity().getInputName(), "ZIW_" + this.node.getInnerEntity().getInputName(), this.node.getInnerTable().getId()));
        return predicate;
    }
    
    public TaskNode translate() {
        final String parent = this.node.getOuterTable().getId();
        final String predicate = this.getFormattedPredicate();
        this.start(this.node.getId()).select((PipelineNode)this.node, entity -> String.format("%s as %s", entity.getInputName(), entity.getOutputName()), i -> this.node.getInputEntitiesUsedByPortId(this.requestContext.getProcessingContext().isValidationMode(), this.node.getOuterPortId()).stream()).from(parent);
        this.builder.append(String.format(" WHERE %s", predicate));
        this.end();
        return null;
    }
}
