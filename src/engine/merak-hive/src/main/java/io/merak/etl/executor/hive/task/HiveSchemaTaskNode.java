package io.merak.etl.executor.hive.task;

import io.merak.etl.sdk.pipeline.*;
import io.merak.etl.task.impl.*;

public class HiveSchemaTaskNode extends AbstractTaskNode
{
    private static final String RESULT_SET = "resultSet";
    private String createOutputViewQuery;
    private String dropOutputViewQuery;
    private final EndVertex selectTargetNode;
    
    public HiveSchemaTaskNode(final String createOutputViewQuery, final String dropOutputViewQuery, final EndVertex selectTargetNode) {
        this.createOutputViewQuery = createOutputViewQuery;
        this.dropOutputViewQuery = dropOutputViewQuery;
        this.selectTargetNode = selectTargetNode;
    }
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public EndVertex getSelectTargetNode() {
        return this.selectTargetNode;
    }
    
    public String getCreateOutputViewQuery() {
        return this.createOutputViewQuery;
    }
    
    public String getDropOutputViewQuery() {
        return this.dropOutputViewQuery;
    }
}
