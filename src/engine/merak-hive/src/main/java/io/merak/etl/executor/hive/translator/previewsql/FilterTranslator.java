package io.merak.etl.executor.hive.translator.previewsql;

import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import com.google.common.base.*;

public class FilterTranslator extends NodeSqlTranslator
{
    private final FilterNode node;
    
    public FilterTranslator(final FilterNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        String predicate = this.node.getOriginalExpression();
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        if (this.node.hasAnalyticsFunctions()) {
            final String newParent = this.node.getName() + "_injected";
            final String conditionColumn = "iw_" + this.node.getName() + "_condition";
            this.start(newParent);
            this.builder.append(String.format("SELECT *, %s AS `%s`", predicate, conditionColumn));
            this.from(parent);
            this.end();
            parent = newParent;
            predicate = conditionColumn + "=true";
        }
        this.start(this.node.getName());
        this.select((PipelineNode)this.node).from(parent);
        if (!Strings.isNullOrEmpty(predicate)) {
            this.builder.append(" WHERE ").append(predicate);
        }
        this.end();
        return (TaskNode)new QueryTaskNode(this.builder.toString());
    }
}
