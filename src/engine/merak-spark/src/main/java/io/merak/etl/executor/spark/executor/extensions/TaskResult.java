package io.merak.etl.executor.spark.executor.extensions;

import io.merak.etl.sdk.executor.*;
import java.util.concurrent.atomic.*;

public class TaskResult implements TaskOutput
{
    private AtomicLong numOfTempTables;
    
    public TaskResult() {
        this.numOfTempTables = new AtomicLong(0L);
    }
    
    public long incrementTempTableCount() {
        return this.numOfTempTables.incrementAndGet();
    }
    
    public Long getTempTableCount() {
        return this.numOfTempTables.get();
    }
}
