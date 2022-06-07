package io.merak.etl.executor.hive.translator;

import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class ExistsTranslator extends SqlTranslator
{
    private final ExistsNode node;
    
    public ExistsTranslator(final ExistsNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        this.start(this.node.getId()).select((PipelineNode)this.node, entity -> String.format("%s as %s", entity.getInputName(), entity.getOutputName()), i -> this.node.getInputEntitiesUsedByPortId(this.requestContext.getProcessingContext().isValidationMode(), this.node.getOuterPortId()).stream()).from(this.node.getOuterTable().getId());
        final String innerTableId = this.node.getInnerTable().getId();
        final String expression = this.node.getExpression();
        this.builder.append(String.format(" WHERE %s (SELECT 1 from %s WHERE %s)", this.node.getType(), innerTableId, expression));
        this.end();
        return null;
    }
}
