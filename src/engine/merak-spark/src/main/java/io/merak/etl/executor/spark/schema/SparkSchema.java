package io.merak.etl.executor.spark.schema;

import io.merak.etl.schema.*;
import io.merak.etl.context.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.executor.spark.session.*;

import org.apache.spark.sql.catalyst.analysis.*;
import merak.tools.ExceptionHandling.*;
import io.merak.etl.utils.column.*;
import com.google.common.collect.*;
import org.apache.spark.sql.*;
import java.util.*;
import com.google.common.base.*;
import org.apache.spark.sql.types.*;
import org.slf4j.*;

public class SparkSchema implements BaseSchemaHandler
{
    private static final Logger LOGGER;
    private SparkSession sparkSession;
    
    public boolean checkTable(final RequestContext requestContext, final String schemaName, final String tableName) {
        this.sparkSession = DFSparkSession.getInstance(requestContext).getSpark();
        try {
            if (this.sparkSession.catalog().listDatabases().filter(String.format("name = '%s'", schemaName)).count() > 0L) {
                return this.sparkSession.catalog().listTables(schemaName).filter(String.format("name = '%s'", tableName)).count() > 0L;
            }
        }
        catch (NoSuchDatabaseException | NoSuchTableException ex2) {
            final AnalysisException ex;
            final AnalysisException e = ex;
            SparkSchema.LOGGER.warn("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
        }
        catch (Exception e2) {
            SparkSchema.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e2));
            Throwables.propagate((Throwable)new IWRunTimeException("{SPARK_TABLE_METADATA_ERROR}", (Throwable)e2));
        }
        return false;
    }
    
    public List<TableColumn> getTableSchema(final RequestContext requestContext, final String schemaName, final String tableName, final boolean hiddenColumns) {
        this.sparkSession = DFSparkSession.getInstance(requestContext).getSpark();
        return this.getTableSchema(schemaName, tableName, hiddenColumns);
    }
    
    public List<TableColumn> getObjectSchema(final Object dataset) {
        return this.fetchDataSetSchema((Dataset)dataset);
    }
    
    public List<TableColumn> getTableSchema(final String schema, final String table, final boolean fetchHiddenColumns) {
        final String sourceQuery = String.format("DESCRIBE `%s`.`%s`", schema, table);
        return this.fetchSchema(sourceQuery, fetchHiddenColumns);
    }
    
    private List<TableColumn> fetchSchema(final String sourceQuery, final boolean fetchHiddenColumns) {
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newLinkedList();
        final Dataset<Row> dataset = (Dataset<Row>)this.sparkSession.sql(sourceQuery);
        final List<Row> rows = (List<Row>)dataset.collectAsList();
        for (final Row row : rows) {
            final String name = row.getString(0);
            String type = row.getString(1).toLowerCase();
            final int i = (type != null && type.length() > 0) ? type.indexOf("(") : -1;
            type = ((i != -1) ? type.substring(0, type.indexOf("(")) : ((type == null) ? "string" : type));
            if (!fetchHiddenColumns && AwbUtil.isHiddenColumn(name)) {
                continue;
            }
            if (name == null || type == null || name.isEmpty()) {
                break;
            }
            if (type.isEmpty()) {
                break;
            }
            final TableColumn column = new TableColumn(name, type);
            colNameTypes.add(column);
        }
        return colNameTypes;
    }
    
    private List<TableColumn> fetchDataSetSchema(final Dataset dataSet) {
        Preconditions.checkNotNull((Object)dataSet, (Object)"Cannot extract schema on null dataset");
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newLinkedList();
        final StructType schema = dataSet.schema();
        for (final StructField field : schema.fields()) {
            final String fieldName = field.name();
            String type = field.dataType().simpleString();
            final int i = (type != null && type.length() > 0) ? type.indexOf("(") : -1;
            type = ((i != -1) ? type.substring(0, type.indexOf("(")) : ((type == null) ? "string" : type));
            final TableColumn tc = new TableColumn(fieldName, type);
            colNameTypes.add(tc);
        }
        return colNameTypes;
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)SparkSchema.class);
    }
}
