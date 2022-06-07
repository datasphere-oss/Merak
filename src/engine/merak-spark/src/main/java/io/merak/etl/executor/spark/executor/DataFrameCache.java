package io.merak.etl.executor.spark.executor;

import java.util.*;
import org.apache.spark.sql.*;
import com.google.common.collect.*;

public class DataFrameCache
{
    private static Map<String, Dataset<Row>> datasetMap;
    
    public static void addToCache(final String key, final Dataset<Row> dataset) {
        DataFrameCache.datasetMap.put(key, dataset);
    }
    
    public static Dataset<Row> getDataSet(final String key) {
        return DataFrameCache.datasetMap.get(key);
    }
    
    public static void clearCaches() {
        DataFrameCache.datasetMap.forEach((k, v) -> v.unpersist());
    }
    
    static {
        DataFrameCache.datasetMap = (Map<String, Dataset<Row>>)Maps.newHashMap();
    }
}
