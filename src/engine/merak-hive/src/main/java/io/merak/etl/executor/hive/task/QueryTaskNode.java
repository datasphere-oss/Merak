package io.merak.etl.executor.hive.task;

import io.merak.etl.task.impl.*;

public class QueryTaskNode extends AbstractTaskNode
{
    protected final String query;
    
    public QueryTaskNode(final String query) {
        this.query = query;
    }
    
    public String getQuery() {
        return this.query;
    }
    
    public String toString() {
        return "QueryTaskNode{priority=1, id='" + this.id + '\'' + ", query='" + this.query + '\'' + '}';
    }
}
