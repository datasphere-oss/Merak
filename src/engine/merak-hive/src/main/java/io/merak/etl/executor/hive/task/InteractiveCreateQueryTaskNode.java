package io.merak.etl.executor.hive.task;

import io.merak.etl.pipeline.dto.*;

public class InteractiveCreateQueryTaskNode extends QueryTaskNode
{
    private final TargetNode createTargetNode;
    
    public InteractiveCreateQueryTaskNode(final String query, final TargetNode createTargetNode) {
        super(query);
        this.createTargetNode = createTargetNode;
    }
    
    public TargetNode getCreateTargetNode() {
        return this.createTargetNode;
    }
    
    @Override
    public String toString() {
        return "InteractiveCreateQueryTaskNode{priority=1, id='" + this.id + '\'' + ", node='" + this.createTargetNode.getId() + '\'' + '}';
    }
}
