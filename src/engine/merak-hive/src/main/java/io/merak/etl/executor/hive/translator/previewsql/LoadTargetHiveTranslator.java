package io.merak.etl.executor.hive.translator.previewsql;

import java.util.function.*;

import merak.tools.ExceptionHandling.*;
import io.merak.etl.context.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.date.*;
import io.merak.etl.utils.shared.*;

import java.util.*;
import com.google.common.collect.*;

import java.sql.*;

public abstract class LoadTargetHiveTranslator<T extends LoadTarget> extends SinkTranslator implements LoadTargetTranslator
{
    protected T node;
    protected String parentNodeName;
    protected String schemaExtractTableName;
    protected String schemaName;
    
    public LoadTargetHiveTranslator(final T node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.parentNodeName = null;
        this.node = node;
        this.schemaName = node.getSchemaName();
        this.schemaExtractTableName = "iw_schema_extract_" + node.getTableName();
    }
    
    protected Map<String, String> resolveAuditColumn(final Map<String, String> expressions) {
        if (!this.node.addAuditColumns()) {
            return expressions;
        }
        expressions.put("ZIW_ROW_ID".toLowerCase(), this.getRowIdExpr());
        expressions.put("ZIW_STATUS_FLAG".toLowerCase(), "'I'");
        expressions.put("ZIW_UPDATED_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        expressions.put("ZIW_IS_DELETED".toLowerCase(), "'false'");
        expressions.put("ZIW_CREATED_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        expressions.put("ZIW_TARGET_START_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        expressions.put("ZIW_TARGET_END_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(DateUtil.getCurrentTimestamp()));
        expressions.put("ZIW_ACTIVE".toLowerCase(), "true");
        expressions.put("ziw_sec_p".toLowerCase(), "cast('0' as int)");
        return expressions;
    }
    
    public String getRowIdExpr() {
        return ((KeyValueTargetNode)this.node).getKeyEntity().getInputName().isEmpty() ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", ((KeyValueTargetNode)this.node).getKeyEntity().getOutputName());
    }
    
    public boolean injectAuditColumns() {
        this.parentNodeName = this.getParent((PipelineNode)this.node);
        if (this.node.isInjected()) {
            return false;
        }
        final String newNodeName = this.node.getId();
        this.start(newNodeName);
        this.select().from(this.parentNodeName).end(this.builder);
        this.parentNodeName = newNodeName;
        return true;
    }
    
    protected NodeSqlTranslator select() {
        final Map<String, String> expressions = (Map<String, String>)Maps.newLinkedHashMap();
        this.resolveAuditColumn(expressions);
        final String auditExpression;
        return this.select((PipelineNode)this.node, c -> {
            auditExpression = expressions.get(c.getGivenName());
            if (auditExpression == null) {
                return this.defaultSelectFormatter(c);
            }
            else {
                return String.format("%s %s", auditExpression, c.getOutputName((PipelineNode)this.node));
            }
        });
    }
    
    public Map<String, String> getSchema(final RequestContext reqCtx, final String schemaName, final String tableName, final Function<String, String> typeOverwrite) {
        final Map<String, String> colNameTypes = (Map<String, String>)Maps.newHashMap();
        try {
            this.LOGGER.debug("Checking out shared connection");
            SharedConnection.execute(reqCtx, reqCtx.getPipelineId(), sharedConn -> {
                Statement stmt;
                final Throwable t2;
                final Function<List<String>, Boolean> executeStmts = (Function<List<String>, Boolean>)(extractTableCreationStmts -> {
                    extractTableCreationStmts.forEach(sqlStmt -> {
                        this.LOGGER.debug("Executing {}", (Object)sqlStmt);
                        try {
                            stmt = sharedConn.createStatement();
                            try {
                                stmt.execute(sqlStmt);
                            }
                            catch (Throwable t) {
                                throw t;
                            }
                            finally {
                                if (stmt != null) {
                                    if (t2 != null) {
                                        try {
                                            stmt.close();
                                        }
                                        catch (Throwable t3) {
                                            t2.addSuppressed(t3);
                                        }
                                    }
                                    else {
                                        stmt.close();
                                    }
                                }
                            }
                        }
                        catch (Exception e) {
                            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                            IWRunTimeException.propagate(e, "HIVE_BATCH_EXECUTION_ERROR");
                        }
                        return;
                    });
                    return Boolean.valueOf(true);
                });
                try {
                    executeStmts.apply(TranslatorUtils.getCreateDatabaseStatement(reqCtx, this.node.getSchemaName()));
                }
                catch (Exception e2) {
                    this.LOGGER.warn("failed while creating database {}", (Object)e2.getMessage());
                }
                final List<String> extractTableCreationStmts2 = this.getSchemaExtractTableCreationStmts();
                executeStmts.apply(extractTableCreationStmts2);
                try (final Statement stmt2 = sharedConn.createStatement()) {
                    final String describeTable = this.getSchemaExtractTableDescribeStmt();
                    this.LOGGER.debug("Executing describing schema extract table {}", (Object)describeTable);
                    try (final ResultSet describeRS = stmt2.executeQuery(describeTable)) {
                        while (describeRS.next()) {
                            final String type = describeRS.getString(2);
                            final String newType = typeOverwrite.apply(type);
                            this.LOGGER.info("inside describe column name is {} type is {} newType {}", new Object[] { describeRS.getString(1), type, newType });
                            colNameTypes.put(describeRS.getString(1), newType);
                        }
                    }
                }
                final String dropTableStmt = this.getSchemaExtractViewDropStmt();
                this.LOGGER.debug("Dropping schema extract table {}", (Object)dropTableStmt);
                try (final Statement stmt3 = sharedConn.createStatement()) {
                    stmt3.execute(dropTableStmt);
                }
            });
        }
        catch (Exception e) {
            this.LOGGER.error("Error while trying to extract schema for {}.{}", (Object)schemaName, (Object)tableName);
            this.LOGGER.error("Exception {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
            IWRunTimeException.propagate(e, "HIVE_BATCH_EXECUTION_ERROR");
        }
        return colNameTypes;
    }
    
    public String getSchemaExtractViewDropStmt() {
        final String dropSchemaViewStmt = String.format("DROP VIEW IF EXISTS `%s`.`%s`", this.schemaName, this.schemaExtractTableName);
        this.LOGGER.debug("Stmt for dropping schema extract table {}", (Object)dropSchemaViewStmt);
        return dropSchemaViewStmt;
    }
    
    public List<String> getSchemaExtractTableCreationStmts() {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add(this.getSchemaExtractViewDropStmt());
        final String dummyTableCreate = String.format("CREATE VIEW `%s`.`%s` AS %s SELECT * FROM %s LIMIT 0", this.schemaName, this.schemaExtractTableName, this.builder.toString(), this.node.getId());
        this.LOGGER.debug("Creating dummy schema extract table {}", (Object)dummyTableCreate);
        stmts.add(dummyTableCreate);
        return stmts;
    }
    
    public String getSchemaExtractTableDescribeStmt() {
        final String describeTable = String.format("DESCRIBE `%s`.`%s`", this.schemaName, this.schemaExtractTableName);
        this.LOGGER.debug("Describing dummy schema extract table {}", (Object)describeTable);
        return describeTable;
    }
}
