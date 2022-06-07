package io.merak.etl.executor.hive.translator;

import java.util.stream.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import java.util.*;

public class UnpivotTranslator extends SqlTranslator
{
    private final UnpivotNode node;
    
    public UnpivotTranslator(final UnpivotNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.node = node;
    }
    
    private SqlTranslator unpivot() {
        final String deriveNames = (String)this.node.getProperties().getDerives().get(0).getDerivedFields().stream().map(c -> c.getOutputName((PipelineNode)this.node)).collect(Collectors.joining(", "));
        this.builder.append(String.format(" LATERAL VIEW explode(map(%s)) %s_t AS %s ", this.getMapExpression(), this.node.getId(), deriveNames));
        return this;
    }
    
    private String getMapExpression() {
        final List<String> mapColumns = (List<String>)this.node.getUnpivotEntities().stream().map(i -> String.format("'%s',%s", i.getGivenName(), i.getOutputName((PipelineNode)this.node))).collect(Collectors.toList());
        final String mapExpression = String.join(",", mapColumns);
        return mapExpression;
    }
    
    public TaskNode translate() {
        String parent = this.getParent((PipelineNode)this.node);
        parent = this.appendDerivations((PipelineNode)this.node, parent);
        this.start(this.node.getId()).select((PipelineNode)this.node).from(parent);
        this.unpivot().end();
        return null;
    }
}
