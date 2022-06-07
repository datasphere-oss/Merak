package io.merak.etl.executor.hive.translator;

import com.google.common.base.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

public class FilterTranslator extends SqlTranslator
{
    private final FilterNode node;
    
    public FilterTranslator(final FilterNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        String predicate = this.node.getExpression();
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        if (this.node.hasAnalyticsFunctions()) {
            final String newParent = this.node.getId() + "_injected";
            final String conditionColumn = "iw_" + this.node.getId() + "_condition";
            this.start(newParent);
            this.builder.append(String.format("SELECT *, %s AS %s", predicate, conditionColumn));
            this.from(parent);
            this.end();
            parent = newParent;
            predicate = conditionColumn + "=true";
        }
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parent);
        if (!Strings.isNullOrEmpty(predicate)) {
            this.builder.append(" WHERE ").append(predicate);
        }
        this.end();
        return null;
    }
}
