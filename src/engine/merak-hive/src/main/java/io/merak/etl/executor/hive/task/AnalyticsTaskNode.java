package io.merak.etl.executor.hive.task;

import java.util.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.predictor.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.sdk.translator.*;

public class AnalyticsTaskNode implements TaskNode
{
    private static final String RESULT_SET = "resultSet";
    private final int priority = 1;
    private final String id;
    private AnalyticsNode analyticsNode;
    private Predictor predictor;
    private String sampleDataQuery;
    private TranslatorState translatorState;
    
    public AnalyticsTaskNode(final AnalyticsNode analyticsNode, final Predictor predictor, final String sampleDataQuery, final TranslatorState translatorState) {
        this.id = UUID.randomUUID().toString();
        this.analyticsNode = analyticsNode;
        this.predictor = predictor;
        this.sampleDataQuery = sampleDataQuery;
        this.translatorState = translatorState;
    }
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public int getPriority() {
        return 1;
    }
    
    public String getId() {
        return this.id;
    }
    
    public AnalyticsNode getAnalyticsNode() {
        return this.analyticsNode;
    }
    
    public Predictor getPredictor() {
        return this.predictor;
    }
    
    public String getSampleDataQuery() {
        return this.sampleDataQuery;
    }
    
    public TranslatorState getTranslatorState() {
        return this.translatorState;
    }
    
    @Override
    public String toString() {
        return "AnalyticsTaskNode {priority=1, id='" + this.id + '\'' + ", analyticsNode='" + this.analyticsNode.getName() + '\'' + "}";
    }
}
