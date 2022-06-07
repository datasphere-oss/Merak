package io.merak.etl.executor.spark.translator;

import io.merak.etl.sdk.translator.*;
import java.util.*;
import io.merak.etl.translator.*;
import io.merak.etl.pipeline.nodes.*;
import io.merak.etl.utils.constants.*;
import io.merak.etl.pipeline.dto.pivot.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.engine.*;

public class SparkTranslatorStore implements TranslatorStore
{
    public void populate(final Map<Class, Class> map) {
        final TranslatorMap<Class, Class> translatorMap = (TranslatorMap<Class, Class>)map;
        translatorMap.put(this.getBatchEngine(), (Object)AggregateNode.class, (Object)AggregateTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)DeriveNode.class, (Object)DeriveTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)FilterNode.class, (Object)FilterTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)JoinNode.class, (Object)JoinTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)DropTableNode.class, (Object)DropTableTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)CleanseNode.class, (Object)CleanseTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)FuzzyMatchingNode.class, (Object)FuzzyMatchingTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)DistinctNode.class, (Object)DistinctTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)UnionNode.class, (Object)UnionTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)UnnestNode.class, (Object)UnnestTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)InNotInNode.class, (Object)InNotInTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)ExistsNode.class, (Object)ExistsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)UnpivotNode.class, (Object)UnpivotTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)CustomTransformationNode.class, (Object)CustomTransformationTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)CustomSourceNode.class, (Object)CustomSourceTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)CustomTargetNode.class, (Object)CustomTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsLogisticRegressionNode.class, (Object)SparkAnalyticsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsDecisionTreeNode.class, (Object)SparkAnalyticsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsKMeansClusteringNode.class, (Object)SparkAnalyticsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsRandomForestClassificationNode.class, (Object)SparkAnalyticsTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsModelExportNode.class, (Object)SparkAnalyticsExportTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)AnalyticsModelImportNode.class, (Object)SparkAnalyticsTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.BATCH, (Object)SnowFlakeTargetNode.class, (Object)SnowFlakeTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)SnowFlakeTargetNode.class, (Object)InteractiveLoadTargetSparkTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.BATCH, (Object)AzureDWNode.class, (Object)AzureDWTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)AzureDWNode.class, (Object)InteractiveLoadTargetSparkTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)SourceNode.class, (Object)SourceTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.BATCH, (Object)SourceNode.class, (Object)BatchSourceTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.INTERACTIVE, (Object)TargetNode.class, (Object)InteractiveTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), AwbConstants.BuildMode.BATCH, (Object)TargetNode.class, (Object)BatchTargetTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)PivotNode.class, (Object)PivotTranslator.class);
        translatorMap.put(this.getBatchEngine(), (Object)LookupNode.class, (Object)LookupTranslator.class);
    }
    
    public BatchEngine getBatchEngine() {
        return BatchEngine.SPARK;
    }
}
