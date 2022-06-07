package io.merak.etl.executor.hive.task;

import io.merak.etl.sdk.pipeline.*;

public class InteractiveSelectQueryTaskNode extends QueryTaskNode
{
    private static final String RESULT_SET = "resultSet";
    private final SinkVertex selectTargetNode;
    
    public InteractiveSelectQueryTaskNode(final String query, final SinkVertex selectTargetNode) {
        super(query);
        this.selectTargetNode = selectTargetNode;
    }
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public SinkVertex getSelectTargetNode() {
        return this.selectTargetNode;
    }
    
    @Override
    public String toString() {
        return "InteractiveSelectQueryTaskNode{priority=1, id='" + this.id + '\'' + ", node='" + this.selectTargetNode.getId() + '\'' + '}';
    }
}
