package io.merak.etl.executor.hive.task;

import io.merak.etl.pipeline.dto.*;

public class LoadTargetInteractiveSelectQueryTaskNode extends QueryTaskNode
{
    private static final String RESULT_SET = "resultSet";
    private final LoadTarget selectTargetNode;
    
    public LoadTargetInteractiveSelectQueryTaskNode(final String query, final LoadTarget selectTargetNode) {
        super(query);
        this.selectTargetNode = selectTargetNode;
    }
    
    public String getOutputKey() {
        return "resultSet";
    }
    
    public LoadTarget getSelectTargetNode() {
        return this.selectTargetNode;
    }
    
    @Override
    public String toString() {
        return "InteractiveSelectQueryTaskNode{priority=1, id='" + this.id + '\'' + ", node='" + this.selectTargetNode.getId() + '\'' + '}';
    }
}
