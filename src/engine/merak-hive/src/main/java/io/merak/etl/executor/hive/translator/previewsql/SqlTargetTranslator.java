package io.merak.etl.executor.hive.translator.previewsql;

import java.util.stream.*;
import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.date.*;

import java.util.*;

public abstract class SqlTargetTranslator extends SinkTranslator implements TargetTranslator
{
    protected final TargetNode node;
    protected String parentNodeName;
    private String currentTimestamp;
    
    public SqlTargetTranslator(final TargetNode node, final TranslatorContext translatorContext) {
        super(translatorContext);
        this.parentNodeName = null;
        this.currentTimestamp = DateUtil.getCurrentTimestamp();
        this.node = node;
    }
    
    public boolean injectAuditColumns() {
        this.parentNodeName = this.getParent((PipelineNode)this.node);
        if (this.node.isInjected()) {
            return false;
        }
        final String newNodeName = this.node.getName();
        this.start(newNodeName);
        this.select().from(this.parentNodeName).end();
        this.parentNodeName = newNodeName;
        return true;
    }
    
    public String getRowIdExpr() {
        return (this.node.getNaturalKeyColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", `%s` ))", this.node.getNaturalKeyColumns().stream().map(column -> String.format("nvl(cast(`%s` as string),\"NULL\")", column.getGivenName())).collect(Collectors.joining(", ")));
    }
    
    public String getSecondaryColumnExpression() {
        return (this.node.getSecondaryPartitionColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", `%s` ))", this.node.getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(`%s` as string),\"NULL\")", column.getGivenName())).collect(Collectors.joining(", ")));
    }
    
    public String getSecondaryColumnExpressionForInteractive() {
        this.LOGGER.debug("Secondary Partition column is : {}", (Object)this.node.getSecondaryPartitionColumns());
        return (this.node.getSecondaryPartitionColumns().size() == 0) ? "" : String.format("hex(concat_ws(\":##:\", `%s` ))", this.node.getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(`%s` as string),\"NULL\")", column.getGivenName())).collect(Collectors.joining(", ")));
    }
    
    protected Map<String, String> resolveAuditColumn(final Map<String, String> expressions) {
        expressions.put("ZIW_ROW_ID".toLowerCase(), this.getRowIdExpr());
        expressions.put("ZIW_STATUS_FLAG".toLowerCase(), "'I'");
        expressions.put("ZIW_UPDATED_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(this.currentTimestamp));
        if (this.node.getDeletedRecordsColumn() == null) {
            expressions.put("ZIW_IS_DELETED".toLowerCase(), "'false'");
        }
        else {
            expressions.put("ZIW_IS_DELETED".toLowerCase(), this.node.getDeletedRecordsColumn().getInputName());
        }
        if (!this.node.isSCD2()) {
            expressions.put("ZIW_CREATED_TIMESTAMP".toLowerCase(), DateUtil.getFormattedTimestampForHive(this.currentTimestamp));
        }
        else {
            final String SCD2Granularity = this.node.getSCD2Granularity().toString();
            expressions.put("ZIW_TARGET_START_TIMESTAMP".toLowerCase(), DateUtil.getStartTimestampFromGranularityHive(this.currentTimestamp, SCD2Granularity));
            expressions.put("ZIW_TARGET_END_TIMESTAMP".toLowerCase(), DateUtil.getInfinityTimestamp());
            expressions.put("ZIW_ACTIVE".toLowerCase(), "true");
        }
        return expressions;
    }
    
    protected NodeSqlTranslator select() {
        final Map<String, String> expressions = (Map<String, String>)Maps.newLinkedHashMap();
        this.resolveAuditColumn(expressions);
        final String auditExpression;
        this.select((PipelineNode)this.node, c -> {
            auditExpression = expressions.get(c.getGivenName());
            if (auditExpression == null) {
                return this.defaultSelectFormatter(c);
            }
            else {
                return String.format("%s %s", auditExpression, c.getGivenName());
            }
        });
        return this;
    }
    
    protected void appendMergeStatement(final String parentNodeName) {
        this.appendMergeStatementImpl(this.builder, String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName()), parentNodeName, true);
    }
    
    protected void appendMergeStatement(final StringBuilder builder, final String mainTable, final String deltaTable) {
        this.appendMergeStatementImpl(builder, String.format("`%s`.`%s`", this.node.getSchemaName(), mainTable), String.format("`%s`.`%s`", this.node.getSchemaName(), deltaTable), false);
    }
    
    private void appendMergeStatementImpl(final StringBuilder builder, final String mainTable, final String deltaTable, final boolean includePrimaryPartitions) {
        // 
        // This method could not be decompiled.
        // 
        // Original Bytecode:
        // 
        //     1: ldc             "SELECT "
        //     3: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //     6: pop            
        //     7: aload_0         /* this */
        //     8: getfield        io/merak/awb/exec/hive/translator/previewsql/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    11: aload_0         /* this */
        //    12: getfield        io/merak/awb/exec/hive/translator/previewsql/SqlTargetTranslator.requestContext:Lio/merak/awb/context/RequestContext;
        //    15: invokevirtual   io/merak/awb/context/RequestContext.getProcessingContext:()Lio/merak/awb/context/ProcessingContext;
        //    18: invokevirtual   io/merak/awb/context/ProcessingContext.isValidationMode:()Z
        //    21: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntitiesUsed:(Z)Ljava/util/stream/Stream;
        //    24: aload_0         /* this */
        //    25: aload_1         /* builder */
        //    26: invokedynamic   BootstrapMethod #4, accept:(Lio/merak/awb/exec/hive/translator/previewsql/SqlTargetTranslator;Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //    31: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //    36: iload           includePrimaryPartitions
        //    38: ifeq            59
        //    41: aload_0         /* this */
        //    42: getfield        io/merak/awb/exec/hive/translator/previewsql/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    45: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getPrimaryPartitionColumns:()Ljava/util/List;
        //    48: aload_1         /* builder */
        //    49: invokedynamic   BootstrapMethod #5, accept:(Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //    54: invokeinterface java/util/List.forEach:(Ljava/util/function/Consumer;)V
        //    59: aload_1         /* builder */
        //    60: aload_1         /* builder */
        //    61: invokevirtual   java/lang/StringBuilder.length:()I
        //    64: iconst_2       
        //    65: isub           
        //    66: invokevirtual   java/lang/StringBuilder.setLength:(I)V
        //    69: aload_1         /* builder */
        //    70: ldc             " FROM `%s` as T"
        //    72: iconst_1       
        //    73: anewarray       Ljava/lang/Object;
        //    76: dup            
        //    77: iconst_0       
        //    78: aload_2         /* mainTable */
        //    79: aastore        
        //    80: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //    83: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    86: pop            
        //    87: aload_1         /* builder */
        //    88: ldc             " FULL OUTER JOIN `%s` as D"
        //    90: iconst_1       
        //    91: anewarray       Ljava/lang/Object;
        //    94: dup            
        //    95: iconst_0       
        //    96: aload_3         /* deltaTable */
        //    97: aastore        
        //    98: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   101: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   104: pop            
        //   105: aload_1         /* builder */
        //   106: ldc             " ON (T.ziw_row_id = D.ziw_row_id)"
        //   108: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   111: pop            
        //   112: return         
        //    StackMapTable: 00 01 3B
        // 
        // The error that occurred was:
        // 
        // java.lang.NullPointerException
        //     at com.strobel.decompiler.languages.java.ast.NameVariables.generateNameForVariable(NameVariables.java:264)
        //     at com.strobel.decompiler.languages.java.ast.NameVariables.assignNamesToVariables(NameVariables.java:198)
        //     at com.strobel.decompiler.languages.java.ast.AstMethodBodyBuilder.createMethodBody(AstMethodBodyBuilder.java:276)
        //     at com.strobel.decompiler.languages.java.ast.AstMethodBodyBuilder.createMethodBody(AstMethodBodyBuilder.java:99)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.createMethodBody(AstBuilder.java:782)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.createMethod(AstBuilder.java:675)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.addTypeMembers(AstBuilder.java:552)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.createTypeCore(AstBuilder.java:519)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.createTypeNoCache(AstBuilder.java:161)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.createType(AstBuilder.java:150)
        //     at com.strobel.decompiler.languages.java.ast.AstBuilder.addType(AstBuilder.java:125)
        //     at com.strobel.decompiler.languages.java.JavaLanguage.buildAst(JavaLanguage.java:71)
        //     at com.strobel.decompiler.languages.java.JavaLanguage.decompileType(JavaLanguage.java:59)
        //     at us.deathmarine.luyten.FileSaver.doSaveJarDecompiled(FileSaver.java:192)
        //     at us.deathmarine.luyten.FileSaver.access$300(FileSaver.java:45)
        //     at us.deathmarine.luyten.FileSaver$4.run(FileSaver.java:112)
        //     at java.base/java.lang.Thread.run(Unknown Source)
        // 
        throw new IllegalStateException("An error occurred while decompiling this method.");
    }
    
    public String getOutputNameForProperty(final Entity input) {
        final Optional<Entity> output = this.node.getOutputEntitiesUsed(true).filter(out -> out.getReferencedEntity() != null && !out.getReferenceType().isDerivation() && out.getReferencedEntity().equals((Object)input)).findAny();
        return output.isPresent() ? output.get().getGivenName() : String.format("`%s`", input.getGivenName());
    }
}
