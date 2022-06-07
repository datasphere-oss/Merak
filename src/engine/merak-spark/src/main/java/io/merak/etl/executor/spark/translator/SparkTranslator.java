package io.merak.etl.executor.spark.translator;

import io.merak.etl.translator.*;
import org.slf4j.*;
import java.util.function.*;
import com.google.common.collect.*;
import org.apache.spark.sql.*;

import java.util.stream.*;
import scala.collection.*;
import io.merak.etl.pipeline.dto.*;
import java.util.*;

import io.merak.etl.sdk.task.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.translator.dto.*;
import io.merak.etl.executor.spark.utils.*;

public abstract class SparkTranslator<T extends PipelineNode> extends Translator
{
    protected SparkSession spark;
    protected final SparkTranslatorState sparkTranslatorState;
    protected Logger LOGGER;
    protected T node;
    
    public SparkTranslator(final T node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
        this.node = node;
        this.sparkTranslatorState = (SparkTranslatorState)translatorContext.getTranslatorState().get();
        this.spark = this.sparkTranslatorState.getSpark();
    }
    
    protected Dataset<Row> select(final PipelineNode node, final Dataset<Row> dataFrame, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return this.select(node, this::defaultSelectFormatter, dataFrame, entitiesUsedFormatter);
    }
    
    protected Dataset<Row> select(final PipelineNode node, final Dataset<Row> dataFrame) {
        return this.select(node, this::defaultSelectFormatter, dataFrame);
    }
    
    protected Dataset<Row> select(final PipelineNode node, final Function<Entity, String> formatter, final Dataset<Row> dataFrame) {
        return this.select(node, formatter, "SELECT ", dataFrame, node::getOutputEntitiesUsed);
    }
    
    protected Dataset<Row> select(final PipelineNode node, final Function<Entity, String> formatter, final Dataset<Row> dataFrame, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return this.select(node, formatter, "SELECT ", dataFrame, entitiesUsedFormatter);
    }
    
    protected Dataset<Row> select(final PipelineNode node, final Function<Entity, String> formatter, final String keyword, final Dataset<Row> dataFrame, final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        List<String> columns = (List<String>)Lists.newArrayList();
        Dataset<Row> selectSet = dataFrame;
        if (entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).count() == 0L) {
            this.LOGGER.trace("select 1 statement");
            selectSet = (Dataset<Row>)dataFrame.select(new Column[] { SparkUtils.getColumnFromExp(this.spark, "1") });
        }
        else if (this.canUseStarInSelect(entitiesUsedFormatter)) {
            this.LOGGER.trace("select * statement");
            selectSet = (Dataset<Row>)dataFrame.select(new Column[] { SparkUtils.getColumnFromExp(this.spark, "*") });
        }
        else {
            columns = entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).map((Function<? super Entity, ?>)formatter).filter(i -> i != null && i.length() > 0).collect((Collector<? super Object, ?, List<String>>)Collectors.toList());
            this.LOGGER.trace("Columns ,{}", (Object)columns);
            this.LOGGER.trace("Schema is : {}", (Object)selectSet.schema().treeString());
            selectSet = (Dataset<Row>)selectSet.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, columns));
            if (columns.isEmpty()) {
                selectSet = (Dataset<Row>)dataFrame.select(new Column[] { SparkUtils.getColumnFromExp(this.spark, "1") });
            }
        }
        return selectSet;
    }
    
    protected String defaultSelectFormatter(final Entity c) {
        if (c.isNameUnchanged()) {
            return c.getOutputName(c.getOwnerNode());
        }
        return String.format("%s AS %s", c.getInputName(), c.getOutputName(c.getOwnerNode()));
    }
    
    protected boolean canUseStarInSelect(final Function<Boolean, Stream<Entity>> entitiesUsedFormatter) {
        return entitiesUsedFormatter.apply(this.requestContext.getProcessingContext().isValidationMode()).count() != 0L && this.hasNoRenames((Function)entitiesUsedFormatter);
    }
    
    protected Dataset<Row> appendDerivations(final PipelineNode node, Dataset<Row> deriveSet, final List<String> plan) {
        final List<Derivation> tempDerivation = (List<Derivation>)Lists.newLinkedList();
        final List<Derivation> outputDerivation = (List<Derivation>)Lists.newLinkedList();
        final List<String> derivedExpressions = (List<String>)Lists.newArrayList();
        for (final Derivation derive : node.getDerivations()) {
            if (derive.getDerivationOutputType() == Derivation.DERIVATION_OUTPUT_TYPE.TEMPORARY) {
                tempDerivation.add(derive);
            }
            else if (derive.getDerivationOutputType() == Derivation.DERIVATION_OUTPUT_TYPE.OUTPUT) {
                outputDerivation.add(derive);
            }
            derivedExpressions.add(derive.getExpression());
        }
        deriveSet = this.appendTempDerivation(node, deriveSet, plan, tempDerivation);
        deriveSet = this.appendOutputDerivation(node, deriveSet, plan, outputDerivation);
        plan.add(String.format("Derive Operations : %s", derivedExpressions.toString()));
        return deriveSet;
    }
    
    private Dataset<Row> appendTempDerivation(final PipelineNode node, Dataset<Row> deriveSet, final List<String> plan, final List<Derivation> derivations) {
        for (final Derivation tempDerive : derivations) {
            deriveSet = this.appendSingleDerivation(node, deriveSet, tempDerive, Lists.newArrayList(), false);
        }
        return deriveSet;
    }
    
    private Dataset<Row> appendOutputDerivation(final PipelineNode node, Dataset<Row> deriveSet, final List<String> plan, final List<Derivation> derivations) {
        final List<String> outputDerivationExp = (List<String>)Lists.newLinkedList();
        outputDerivationExp.add("*");
        for (final Derivation outputDerive : derivations) {
            deriveSet = this.appendSingleDerivation(node, deriveSet, outputDerive, outputDerivationExp, true);
        }
        deriveSet = (Dataset<Row>)deriveSet.selectExpr((Seq)SparkUtils.getStringSeqFromList(outputDerivationExp));
        return deriveSet;
    }
    
    private Dataset<Row> appendSingleDerivation(final PipelineNode node, Dataset<Row> deriveSet, final Derivation derive, final List<String> outputExprs, final boolean isOutputDerivation) {
        if (!this.requestContext.getProcessingContext().isValidationMode() && !node.isDeriveUsed(derive)) {
            return deriveSet;
        }
        if (derive.getDerivationOutputCount() == 1) {
            deriveSet = this.deriveColumn(derive.getExpression(), derive.getDerivedFields().get(0).getOutputName(), isOutputDerivation, outputExprs, deriveSet);
        }
        else {
            for (int i1 = 0; i1 < derive.getDerivationOutputCount(); ++i1) {
                final Entity col = derive.getDerivedFields().get(i1);
                final String exp = String.format("(%s)[%d]", derive.getExpression(), i1);
                deriveSet = this.deriveColumn(exp, col.getOutputName(node), isOutputDerivation, outputExprs, deriveSet);
            }
        }
        return deriveSet;
    }
    
    private Dataset<Row> deriveColumn(final String expression, final String columnName, final boolean isOutputDerivation, final List<String> outputExprs, Dataset<Row> deriveSet) {
        if (isOutputDerivation) {
            final String column = String.format("%s as %s", expression, columnName);
            outputExprs.add(column);
        }
        else {
            deriveSet = (Dataset<Row>)deriveSet.withColumn(columnName, SparkUtils.getColumnFromExp(this.spark, expression));
        }
        return deriveSet;
    }
    
    public Dataset<Row> getDataset(final PipelineNode node) {
        final DataFrameObject dataFrameObject = this.sparkTranslatorState.getDataFrame(node);
        if (dataFrameObject != null) {
            return dataFrameObject.getDataset();
        }
        return null;
    }
    
    public TaskNode translate() {
        this.sparkTranslatorState.addTranslations(this::generateDataFrame);
        return null;
    }
    
    protected String sourceSelectFormatter(final Entity c) {
        return String.format("%s AS %s", AwbUtil.escapeSqlIdentifier(c.getGivenName()), c.getOutputName(c.getOwnerNode()));
    }
    
    protected abstract void generateDataFrame();
    
    protected Dataset remapOutputDataset(Dataset outputDataset, final List<Entity> outputEntities) {
        final List<String> aliases = (List<String>)Lists.newArrayList();
        for (final Entity component : outputEntities) {
            aliases.add(String.format(" %s as %s", component.getGivenName(), component.getInputName()));
        }
        outputDataset = outputDataset.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, aliases));
        return outputDataset;
    }
    
    protected Dataset remapInputDataset(Dataset inputDataset, final List<Entity> inputEntities) {
        final List<String> aliases = (List<String>)Lists.newArrayList();
        for (final Entity component : inputEntities) {
            aliases.add(String.format(" %s as %s", component.getInputName(), component.getGivenName()));
        }
        inputDataset = inputDataset.select((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, aliases));
        return inputDataset;
    }
    
    public SparkSession getSpark() {
        return this.spark;
    }
    
    protected Dataset<Row> appendPostActions(final PipelineNode node, Dataset<Row> dataset, final List<String> plan) {
        if (node.getPostProcessingConfigs() != null && node.getPostProcessingConfigs().isDistributionEnabled()) {
            if (node.getPostProcessingConfigs().isDistributionEnabled()) {
                final List<String> repartitionEntityNames = (List<String>)node.getPostProcessingConfigs().getDistributionEntities().stream().map(Entity::getOutputName).collect(Collectors.toList());
                final int partitionNum = node.getPostProcessingConfigs().getDistributionPartitions();
                plan.add(String.format("repartitioning on columns %s into %s partitions ", repartitionEntityNames, partitionNum));
                dataset = (Dataset<Row>)dataset.repartition(partitionNum, (Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, repartitionEntityNames));
                if (node.getPostProcessingConfigs().isSortingEnabled()) {
                    final List<String> sortEntitiesNames = (List<String>)node.getPostProcessingConfigs().getDistributionEntities().stream().map(Entity::getOutputName).collect(Collectors.toList());
                    plan.add(String.format("sortingWithinPartitions on columns %s", sortEntitiesNames));
                    dataset = (Dataset<Row>)dataset.sortWithinPartitions((Seq)SparkUtils.getColumnSeqFromExpressions(this.spark, sortEntitiesNames));
                }
            }
            this.LOGGER.debug("plan {}", (Object)plan.toString());
        }
        return dataset;
    }
}
