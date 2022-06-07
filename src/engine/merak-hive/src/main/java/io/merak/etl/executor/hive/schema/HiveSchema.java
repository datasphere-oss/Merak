package io.merak.etl.executor.hive.schema;

import org.apache.hadoop.hive.conf.*;
import merak.tools.config.*;
import io.merak.etl.context.*;
import io.merak.etl.schema.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.shared.*;

import org.apache.commons.lang3.*;
import org.apache.hadoop.hive.metastore.*;

import java.util.*;

import com.google.common.base.*;
import java.sql.*;
import com.google.common.collect.*;

import org.slf4j.*;

public class HiveSchema implements BaseSchemaHandler
{
    private static final Logger LOGGER;
    private Connection connection;
    private String schema;
    private String table;
    private boolean getHiddenColumns;
    
    public HiveSchema() {
    }
    
    public HiveSchema(final Connection connection) {
        this.connection = connection;
    }
    
    public HiveSchema(final Connection connection, final String schema, final String table, final boolean getHiddenColumns) {
        this.connection = connection;
        this.schema = schema;
        this.table = table;
        this.getHiddenColumns = getHiddenColumns;
    }
    
    public String getSchema() {
        return this.schema;
    }
    
    public void setSchema(final String schema) {
        this.schema = schema;
    }
    
    public String getTable() {
        return this.table;
    }
    
    public void setTable(final String table) {
        this.table = table;
    }
    
    public boolean isGetHiddenColumns() {
        return this.getHiddenColumns;
    }
    
    public void setGetHiddenColumns(final boolean getHiddenColumns) {
        this.getHiddenColumns = getHiddenColumns;
    }
    
    public boolean checkTable(final RequestContext requestContext, final String schemaName, final String tableName) {
        try {
            final HiveConf hiveConf = new HiveConf();
            if (!StringUtils.isEmpty((CharSequence)IWConstants.HIVE_METASTORE_URI)) {
                hiveConf.set("hive.metastore.uris", IWConstants.HIVE_METASTORE_URI);
            }
            HiveSchema.LOGGER.info("Using HiveConf URI: " + hiveConf.get("hive.metastore.uris"));
            final HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(hiveConf);
            return metaStoreClient.tableExists(schemaName, tableName);
        }
        catch (Exception e2) {
            try {
                return this.getTableSchema(requestContext, schemaName, tableName, false) != null;
            }
            catch (Exception e1) {
                HiveSchema.LOGGER.debug("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e1));
                return false;
            }
        }
    }
    
    private List<TableColumn> fetchTableSchema(final String sourceQuery, final List<TableColumn> colNameTypes) {
        try (final Statement stmt = this.connection.createStatement()) {
            try (final ResultSet resultSet = stmt.executeQuery(sourceQuery)) {
                while (resultSet.next()) {
                    final String name = resultSet.getString(1);
                    String type = resultSet.getString(2);
                    final int i = (type != null && type.length() > 0) ? type.indexOf("(") : -1;
                    type = ((i != -1) ? type.substring(0, type.indexOf("(")) : ((type == null) ? "string" : type));
                    if (!this.getHiddenColumns && AwbUtil.isHiddenColumn(name)) {
                        continue;
                    }
                    if (name == null || type == null || name.isEmpty()) {
                        break;
                    }
                    if (type.isEmpty()) {
                        break;
                    }
                    colNameTypes.add(new TableColumn(name, type));
                }
            }
            catch (SQLException e) {
                Throwables.propagate((Throwable)e);
            }
        }
        catch (Exception e2) {
            Throwables.propagate((Throwable)e2);
            HiveSchema.LOGGER.error("Exception while getting schema {}.{}", (Object)this.schema, (Object)this.table);
        }
        return colNameTypes;
    }
    
    public List<TableColumn> getTableSchema(final RequestContext reqCxt, final String schemaName, final String tableName, final boolean fetchHiddenColumns) {
        final List<TableColumn> colNameTypes = (List<TableColumn>)Lists.newArrayList();
        try {
            final String poolingKey = reqCxt.getPoolingCriteria();
            SharedConnection.execute(reqCxt, poolingKey, connection -> {
                this.connection = connection;
                this.schema = schemaName;
                this.table = tableName;
                this.getHiddenColumns = fetchHiddenColumns;
                final String sourceQuery = String.format("DESCRIBE `%s`.`%s`", schemaName, tableName);
                this.fetchTableSchema(sourceQuery, colNameTypes);
            });
        }
        catch (Exception e) {
            Throwables.propagate((Throwable)e);
        }
        return colNameTypes;
    }
    
    public List<TableColumn> getObjectSchema(final Object dataset) {
        return (List<TableColumn>)Lists.newArrayList();
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveSchema.class);
    }
}
