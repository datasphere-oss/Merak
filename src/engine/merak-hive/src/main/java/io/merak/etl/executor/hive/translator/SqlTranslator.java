package io.merak.etl.executor.hive.translator;

import java.util.function.*;
import java.util.stream.*;
import java.util.*;
import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;

public abstract class SqlTranslator extends Translator
{
    protected final StringBuilder builder;
    
    public SqlTranslator(final TranslatorContext translatorContext) {
        super(translatorContext);
        this.builder = (StringBuilder)translatorContext.getTranslatorState().get();
    }
    
    protected SqlTranslator start(final String id) {
        this.builder.append(id).append(" AS (");
        return this;
    }
    
    protected SqlTranslator start(final StringBuilder builder, final String id) {
        builder.append(id).append(" AS (");
        return this;
    }
    
    protected void end() {
        this.builder.append("),\n");
    }
    
    protected void end(final StringBuilder builder) {
        builder.append("),\n");
    }
    
    protected void endWithoutComma(final StringBuilder builder) {
        builder.append(")\n");
    }
    
    protected SqlTranslator select(final PipelineNode node) {
        return this.select(node, this::defaultSelectFormatter);
    }
    
    protected SqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter) {
        return this.select(node, formatter, "SELECT ", node::getOutputEntitiesUsed);
    }
    
    protected SqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return this.select(node, formatter, "SELECT ", entitiesUsedFormatter);
    }
    
    protected SqlTranslator select(final PipelineNode node, final Function<Entity, String> formatter, final String keyword, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        this.builder.append(this.getColumnsFromStream(formatter, keyword, entitiesUsedFormatter));
        return this;
    }
    
    protected String getColumnsFromStream(final Function<Entity, String> formatter, final String keyword, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        final StringBuilder thisBuilder = new StringBuilder();
        thisBuilder.append(keyword);
        String columns;
        if (entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).count() == 0L) {
            columns = "1";
        }
        else if (this.canUseStarInSelect(entitiesUsedFormatter)) {
            columns = "*";
        }
        else {
            columns = entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).map((Function<? super Entity, ?>)formatter).filter(Objects::nonNull).collect((Collector<? super Object, ?, String>)Collectors.joining(", "));
            if (columns.isEmpty()) {
                columns = "1";
            }
        }
        thisBuilder.append(columns);
        return thisBuilder.toString();
    }
    
    protected String defaultSelectFormatter(final Entity c) {
        if (c.isNameUnchanged()) {
            return c.getOutputName(c.getOwnerNode());
        }
        return String.format("%s AS %s", c.getInputName(), c.getOutputName(c.getOwnerNode()));
    }
    
    protected String sourceSelectFormatter(final Entity c) {
        return String.format("%s AS %s", AwbUtil.escapeSqlIdentifier(c.getGivenName()), c.getOutputName(c.getOwnerNode()));
    }
    
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).count() != 0L && this.hasNoRenames((Function)entitiesUsedFormatter);
    }
    
    public SqlTranslator from(final String from) {
        this.builder.append(" FROM ").append(from);
        return this;
    }
    
    protected SqlTranslator from(final StringBuilder builder, final String from) {
        builder.append(" FROM ").append(from);
        return this;
    }
    
    public String appendTemporaryDerivations(final PipelineNode node, String parentNodeName) {
        int i = 0;
        for (final Derivation derive : node.getDerivations()) {
            if (derive.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.OUTPUT)) {
                continue;
            }
            parentNodeName = this.appendSingleDerivation(node, parentNodeName, derive, i++, false);
        }
        return parentNodeName;
    }
    
    public String appendOutputDerivations(final PipelineNode node, String parentNodeName) {
        int i = 0;
        for (final Derivation derive : node.getDerivations()) {
            if (derive.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.TEMPORARY)) {
                continue;
            }
            parentNodeName = this.appendSingleDerivation(node, parentNodeName, derive, i++, true);
        }
        return parentNodeName;
    }
    
    public String appendDerivations(final PipelineNode node, String parentNodeName) {
        if (node.getDerivations().size() == 0) {
            return parentNodeName;
        }
        int i = 0;
        final List<Derivation> outputDerives = (List<Derivation>)node.getDerivations().stream().filter(derivation -> derivation.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.OUTPUT)).collect(Collectors.toList());
        final List<Derivation> tempDerives = (List<Derivation>)node.getDerivations().stream().filter(derivation -> derivation.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.TEMPORARY)).collect(Collectors.toList());
        for (final Derivation tempDerive : tempDerives) {
            parentNodeName = this.appendSingleDerivation(node, parentNodeName, tempDerive, i++, tempDerive.getDerivationOutputType().equals((Object)Derivation.DERIVATION_OUTPUT_TYPE.OUTPUT));
        }
        final List<String> outputDeriveExpressions = (List<String>)Lists.newArrayList();
        for (final Derivation outputDerive : outputDerives) {
            if (!this.requestContext.getProcessingContext().isValidationMode() && !node.isDeriveUsed(outputDerive)) {
                continue;
            }
            if (outputDerive.getDerivationOutputCount() > 1) {
                for (int i2 = 0; i2 < outputDerive.getDerivationOutputCount(); ++i2) {
                    final Entity col = outputDerive.getDerivedFields().get(i2);
                    outputDeriveExpressions.add(String.format("(%s)[%d] AS `%s`", outputDerive.getExpression(), i2, col.getOutputName(node)));
                }
            }
            else {
                outputDeriveExpressions.add(String.format("%s AS `%s`", outputDerive.getExpression(), outputDerive.getDerivedFields().get(0).getOutputName()));
            }
        }
        String outputExpression = "";
        if (outputDeriveExpressions.size() > 0) {
            final String nodeName = this.getNewNodeName(node.getId(), i++, true);
            outputExpression = String.format("%s", outputDeriveExpressions.stream().collect((Collector<? super Object, ?, String>)Collectors.joining(",")));
            this.builder.append(String.format("%s AS (SELECT %s.*, %s FROM %s),\n", nodeName, parentNodeName, outputExpression, parentNodeName));
            return nodeName;
        }
        return parentNodeName;
    }
    
    private String appendSingleDerivation(final PipelineNode node, String parentNodeName, final Derivation derive, int i, final boolean hasOutputDerives) {
        if (!this.requestContext.getProcessingContext().isValidationMode() && !node.isDeriveUsed(derive)) {
            return parentNodeName;
        }
        final String pName = node.getId();
        final String newNodeName = this.getNewNodeName(pName, i++, hasOutputDerives);
        if (derive.getDerivationOutputCount() == 1) {
            this.builder.append(String.format("%s AS (SELECT %s.*, %s AS %s FROM %s),\n", newNodeName, parentNodeName, derive.getExpression(), derive.getDerivedFields().get(0).getOutputName(), parentNodeName));
        }
        else {
            this.builder.append(String.format("%s AS (SELECT %s.*", newNodeName, parentNodeName));
            for (int i2 = 0; i2 < derive.getDerivationOutputCount(); ++i2) {
                final Entity col = derive.getDerivedFields().get(i2);
                this.builder.append(String.format(", (%s)[%d] AS %s", derive.getExpression(), i2, col.getOutputName(node)));
            }
            this.builder.append(String.format(" FROM %s),\n", parentNodeName));
        }
        parentNodeName = newNodeName;
        return parentNodeName;
    }
    
    protected String getNewNodeName(final String pName, final int index, final boolean hasOutputDerives) {
        return String.format("%s_%s%d", pName, hasOutputDerives ? "o" : "t", index);
    }
    
    protected void appendPostActions(final PipelineNode node) {
        if (node.getPostProcessingConfigs() != null && node.getPostProcessingConfigs().isDistributionEnabled()) {
            final String distributeColumns = (String)node.getPostProcessingConfigs().getDistributionEntities().stream().map(e -> e.getOutputName(node)).collect(Collectors.joining(","));
            this.builder.append(String.format(" DISTRIBUTE BY %s ", distributeColumns));
            if (node.getPostProcessingConfigs().isSortingEnabled()) {
                final String sortColumns = (String)node.getPostProcessingConfigs().getSortEntities().stream().map(e -> e.getOutputName(node)).collect(Collectors.joining(","));
                this.builder.append(String.format(" SORT BY %s ", sortColumns));
            }
        }
    }
}
