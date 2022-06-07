package io.merak.etl.executor.hive.translator;

import io.merak.adapters.filesystem.*;
import io.merak.etl.executor.hive.task.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.sdk.task.*;
import io.merak.etl.translator.*;

import org.slf4j.*;
import com.google.common.collect.*;

import java.util.stream.*;
import java.util.*;

public class MapRDBHiveTranslator extends LoadTargetHiveTranslator<LoadTarget>
{
    protected Logger LOGGER;
    private static final String TMP_TAB_SUFFIX = "_iwtmp";
    private static final String VIEW_PREFIX = "iw_schema_extract_";
    private boolean isExistingKeyStore;
    private String mapRDBTableName;
    private KeyValueTargetNode keyValueTargetNode;
    protected String schemaName;
    protected String hdfsLocation;
    protected String schemaExtractTableName;
    protected String givenTableName;
    protected IWFileSystem iwFileSystem;
    protected String workingTableName;
    protected boolean usingTempTable;
    
    public MapRDBHiveTranslator(final LoadTarget node, final TranslatorContext translatorContext) {
        super(node, translatorContext);
        this.LOGGER = LoggerFactory.getLogger((Class)this.getClass());
        this.schemaName = ((MapRDBKeyStoreTarget)node).getImposedSchemaName();
        this.givenTableName = ((MapRDBKeyStoreTarget)node).getImposedTableName();
        this.schemaExtractTableName = String.format("%s%s", "iw_schema_extract_", this.givenTableName);
        this.workingTableName = String.format("%s%s", this.givenTableName, "_iwtmp");
        this.isExistingKeyStore = ((MapRDBKeyStoreTarget)node).isKeyStoreExists();
        this.mapRDBTableName = ((MapRDBKeyStoreTarget)node).getKeyStoreTableName();
        this.parentNodeName = this.getParent((PipelineNode)node);
        this.keyValueTargetNode = (KeyValueTargetNode)node;
        this.iwFileSystem = IWFileSystem.getFileSystemFromPath(this.keyValueTargetNode.getKeyStoreTableName());
    }
    
    public List<String> generateTableStmts(final Map<String, String> columnTypes) {
        final String tableName = this.usingTempTable ? this.workingTableName : this.givenTableName;
        final List<String> stmts = (List<String>)Lists.newArrayList();
        if (this.shouldCreateTable()) {
            stmts.add(String.format("DROP TABLE IF EXISTS `%s`.`%s` PURGE", this.schemaName, tableName));
        }
        return stmts;
    }
    
    protected String getCreateTableStmt(final Map<String, String> columnTypes, String tableName, final String location) {
        final StringBuilder createStmt = new StringBuilder();
        this.schemaName = ((MapRDBKeyStoreTarget)this.node).getImposedSchemaName();
        tableName = ((MapRDBKeyStoreTarget)this.node).getImposedTableName();
        final String external = this.isExistingKeyStore ? "EXTERNAL " : "";
        createStmt.append(String.format("CREATE %sTABLE `%s`.`%s`", external, this.schemaName, tableName));
        this.appendColumns(columnTypes, createStmt);
        createStmt.append(this.getStoredByStmt());
        createStmt.append(this.createTableProperties());
        this.LOGGER.info("getCreateTableStmt is {}", (Object)createStmt.toString());
        return createStmt.toString();
    }
    
    public TaskNode translate() {
        if (!this.injectAuditColumns()) {
            this.start(this.node.getId()).select((PipelineNode)this.node).from(this.parentNodeName).endWithoutComma(this.builder);
        }
        else {
            this.with();
        }
        this.schemaName = ((MapRDBKeyStoreTarget)this.node).getImposedSchemaName();
        this.givenTableName = ((MapRDBKeyStoreTarget)this.node).getImposedTableName();
        this.schemaExtractTableName = "iw_schema_extract_" + this.node.getTableName();
        return (TaskNode)new KeyStoreTargetTaskNode((MapRDBKeyStoreTarget)this.node, this, this.builder.toString());
    }
    
    protected String createTableProperties() {
        final StringBuilder tablePropertiesStmt = new StringBuilder();
        tablePropertiesStmt.append("\nTBLPROPERTIES (\n");
        final List<String> tblPropertStmt = (List<String>)Lists.newArrayList();
        tblPropertStmt.add(this.getMapRDBTableNameStmt());
        tblPropertStmt.add(this.getMapRIdStmt());
        tablePropertiesStmt.append(tblPropertStmt.stream().collect((Collector<? super Object, ?, String>)Collectors.joining(",\n ")));
        tablePropertiesStmt.append("\n)");
        this.LOGGER.info("createTableProperties is {}", (Object)tablePropertiesStmt.toString());
        return tablePropertiesStmt.toString();
    }
    
    protected String getMapRDBTableNameStmt() {
        return String.format("'maprdb.table.name'='%s'", this.mapRDBTableName);
    }
    
    protected String getMapRIdStmt() {
        return String.format("'maprdb.column.id'='%s'", this.getMapRKeyName());
    }
    
    private String getMapRKeyName() {
        return ((MapRDBKeyStoreTarget)this.node).getKeyEntity().getGivenName();
    }
    
    protected String getStoredByStmt() {
        return "STORED BY 'org.apache.hadoop.hive.maprdb.json.MapRDBJsonStorageHandler'\n";
    }
    
    public List<String> getDropTableStatement() {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.add(String.format("DROP TABLE IF EXISTS `%s`.`%s` PURGE", ((MapRDBKeyStoreTarget)this.node).getImposedSchemaName(), ((MapRDBKeyStoreTarget)this.node).getImposedTableName()));
        return stmts;
    }
    
    public String getInsertStmt(final String schemaName, final String tableName) {
        final StringBuilder insertStmt = new StringBuilder();
        insertStmt.append((CharSequence)this.builder);
        final String sequence = this.getColumnSequence();
        insertStmt.append(String.format("INSERT INTO `%s`.`%s`(%s)", schemaName, tableName, sequence));
        insertStmt.append(String.format("\nSELECT %s FROM %s", sequence, this.node.getId()));
        this.LOGGER.info("getInsertStmt is {}", (Object)insertStmt.toString());
        return insertStmt.toString();
    }
    
    private String getColumnSequence() {
        String sequence = "";
        for (final Entity entity : this.node.getOutputEntities()) {
            if (sequence.length() == 0) {
                sequence = String.format("`%s`", entity.getGivenName());
            }
            else {
                sequence = String.format("%s,`%s`", sequence, entity.getGivenName());
            }
        }
        return sequence;
    }
    
    public List<String> getTableCreationForOverWrite(final Map<String, String> columnTypes, final String tableName) {
        final List<String> stmts = (List<String>)Lists.newArrayList();
        stmts.addAll(this.getDropTableStatement());
        stmts.add(this.getCreateTableStmt(columnTypes, tableName, null));
        return stmts;
    }
    
    protected void appendColumns(final Map<String, String> columnTypes, final StringBuilder createStmt) {
        createStmt.append("\n(");
        final List<String> colStmts = (List<String>)Lists.newArrayList();
        this.node.getOutputEntities().forEach(c -> colStmts.add(c.getOutputName((PipelineNode)this.node) + " " + columnTypes.get(c.getGivenName())));
        createStmt.append(String.join(",\n", colStmts));
        createStmt.append("\n)");
    }
    
    public boolean shouldCreateTable() {
        return this.node.getSyncMode() != LoadTarget.SyncMode.APPEND;
    }
}
