package io.merak.etl.executor.hive.translator;

import java.util.stream.*;
import com.google.common.collect.*;

import io.merak.etl.pipeline.dto.*;
import io.merak.etl.translator.*;
import io.merak.etl.utils.column.*;
import io.merak.etl.utils.config.*;
import io.merak.etl.utils.date.*;

import java.util.function.*;
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
        final String newNodeName = this.node.getId();
        this.start(newNodeName);
        this.select().from(this.parentNodeName).end();
        this.parentNodeName = newNodeName;
        return true;
    }
    
    public String getRowIdExpr() {
        return (this.node.getNaturalKeyColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", this.node.getNaturalKeyColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getInputName())).collect(Collectors.joining(", ")));
    }
    
    public String getSecondaryColumnExpression() {
        return (this.node.getSecondaryPartitionColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", this.node.getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getInputName())).collect(Collectors.joining(", ")));
    }
    
    public String getSecondaryColumnExpressionForInteractive() {
        this.LOGGER.debug("Secondary Partition column is : {}", (Object)this.node.getSecondaryPartitionColumns());
        return (this.node.getSecondaryPartitionColumns().size() == 0) ? "uuid()" : String.format("hex(concat_ws(\":##:\", %s ))", this.node.getSecondaryPartitionColumns().stream().map(column -> String.format("nvl(cast(%s as string),\"NULL\")", column.getOutputName((PipelineNode)this.node))).collect(Collectors.joining(", ")));
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
    
    protected SqlTranslator select() {
        final Map<String, String> expressions = (Map<String, String>)Maps.newLinkedHashMap();
        this.resolveAuditColumn(expressions);
        final String auditExpression;
        this.select((PipelineNode)this.node, c -> {
            auditExpression = expressions.get(c.getGivenName());
            if (auditExpression == null) {
                return this.defaultSelectFormatter(c);
            }
            else {
                return String.format("%s %s", auditExpression, c.getOutputName((PipelineNode)this.node));
            }
        }, this.node::getOutputEntitiesUsed);
        return this;
    }
    
    protected void appendMergeStatement(final String parentNodeName) {
        if (!this.node.isSCD2()) {
            this.appendMergeStatementImpl(this.builder, String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName()), parentNodeName, true);
        }
        else {
            this.appendMergeStatementImplSCD2(this.builder, String.format("`%s`.`%s`", this.node.getSchemaName(), this.node.getTableName()), parentNodeName, null, true);
        }
    }
    
    protected void appendMergeStatement(final StringBuilder builder, final String mainTable, final String deltaTable, final String mergedTable) {
        if (!this.node.isSCD2()) {
            this.appendMergeStatementImpl(builder, String.format("`%s`.`%s`", this.node.getSchemaName(), mainTable), String.format("`%s`.`%s`", this.node.getSchemaName(), deltaTable), false);
        }
        else {
            this.appendMergeStatementImplSCD2(builder, String.format("`%s`.`%s`", this.node.getSchemaName(), mainTable), String.format("`%s`.`%s`", this.node.getSchemaName(), deltaTable), String.format("`%s`.`%s`", this.node.getSchemaName(), mergedTable), false);
        }
    }
    
    private void appendMergeStatementImplSCD2(final StringBuilder builder, final String mainTable, final String deltaTable, final String mergedTable, final boolean includePrimaryPartitions) {
        // 
        // This method could not be decompiled.
        // 
        // Original Bytecode:
        // 
        //     2: ifnonnull       30
        //     5: aload_1         /* builder */
        //     6: ldc             "SELECT * FROM %s"
        //     8: iconst_1       
        //     9: anewarray       Ljava/lang/Object;
        //    12: dup            
        //    13: iconst_0       
        //    14: aload_0         /* this */
        //    15: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    18: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getId:()Ljava/lang/String;
        //    21: aastore        
        //    22: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //    25: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    28: pop            
        //    29: return         
        //    30: aload_1         /* builder */
        //    31: ldc             "WITH\n"
        //    33: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    36: pop            
        //    37: ldc             "T"
        //    39: astore          mainTableName
        //    41: ldc             "D"
        //    43: astore          deltaTableName
        //    45: ldc             "TD"
        //    47: astore          joinTableName
        //    49: ldc             "F0"
        //    51: astore          unionTableName
        //    53: aload_0         /* this */
        //    54: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    57: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getSCD2Granularity:()Lio/merak/awb/pipeline/dto/TargetNode$SCD2GRANULARITY;
        //    60: invokevirtual   io/merak/awb/pipeline/dto/TargetNode$SCD2GRANULARITY.toString:()Ljava/lang/String;
        //    63: astore          SCD2Granularity
        //    65: aload_0         /* this */
        //    66: aload_1         /* builder */
        //    67: aload           mainTableName
        //    69: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.start:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //    72: pop            
        //    73: aload_1         /* builder */
        //    74: ldc             "SELECT * FROM %s where ziw_active = true"
        //    76: iconst_1       
        //    77: anewarray       Ljava/lang/Object;
        //    80: dup            
        //    81: iconst_0       
        //    82: aload_2         /* mainTable */
        //    83: aastore        
        //    84: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //    87: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    90: pop            
        //    91: aload_0         /* this */
        //    92: aload_1         /* builder */
        //    93: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.end:(Ljava/lang/StringBuilder;)V
        //    96: aload_0         /* this */
        //    97: aload_1         /* builder */
        //    98: aload           joinTableName
        //   100: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.start:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   103: pop            
        //   104: aload_1         /* builder */
        //   105: ldc             "SELECT D.*, "
        //   107: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   110: pop            
        //   111: aload_0         /* this */
        //   112: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   115: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   118: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   123: aload_0         /* this */
        //   124: invokedynamic   BootstrapMethod #5, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   129: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   134: aload_0         /* this */
        //   135: aload_1         /* builder */
        //   136: invokedynamic   BootstrapMethod #6, accept:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //   141: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   146: aload_1         /* builder */
        //   147: aload_1         /* builder */
        //   148: invokevirtual   java/lang/StringBuilder.length:()I
        //   151: iconst_1       
        //   152: isub           
        //   153: aload_1         /* builder */
        //   154: invokevirtual   java/lang/StringBuilder.length:()I
        //   157: ldc             " "
        //   159: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   162: pop            
        //   163: aload_1         /* builder */
        //   164: ldc             "FROM %s FULL OUTER JOIN %s AS %s ON T.ziw_row_id = D.ziw_row_id"
        //   166: iconst_3       
        //   167: anewarray       Ljava/lang/Object;
        //   170: dup            
        //   171: iconst_0       
        //   172: aload           mainTableName
        //   174: aastore        
        //   175: dup            
        //   176: iconst_1       
        //   177: aload_3         /* deltaTable */
        //   178: aastore        
        //   179: dup            
        //   180: iconst_2       
        //   181: aload           deltaTableName
        //   183: aastore        
        //   184: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   187: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   190: pop            
        //   191: aload_0         /* this */
        //   192: aload_1         /* builder */
        //   193: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.end:(Ljava/lang/StringBuilder;)V
        //   196: aload_0         /* this */
        //   197: aload_1         /* builder */
        //   198: aload           unionTableName
        //   200: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.start:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   203: pop            
        //   204: aload_1         /* builder */
        //   205: ldc             "SELECT "
        //   207: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   210: pop            
        //   211: aload_0         /* this */
        //   212: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   215: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   218: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   223: aload_0         /* this */
        //   224: invokedynamic   BootstrapMethod #7, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   229: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   234: aload_1         /* builder */
        //   235: invokedynamic   BootstrapMethod #8, accept:(Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //   240: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   245: aload_1         /* builder */
        //   246: aload_1         /* builder */
        //   247: invokevirtual   java/lang/StringBuilder.length:()I
        //   250: iconst_1       
        //   251: isub           
        //   252: aload_1         /* builder */
        //   253: invokevirtual   java/lang/StringBuilder.length:()I
        //   256: ldc             " "
        //   258: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   261: pop            
        //   262: aload_0         /* this */
        //   263: aload_1         /* builder */
        //   264: aload           joinTableName
        //   266: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.from:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   269: pop            
        //   270: aload_1         /* builder */
        //   271: ldc             " WHERE TD.%s is NULL"
        //   273: iconst_1       
        //   274: anewarray       Ljava/lang/Object;
        //   277: dup            
        //   278: iconst_0       
        //   279: ldc             "t_ziw_row_id"
        //   281: aastore        
        //   282: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   285: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   288: pop            
        //   289: aload_1         /* builder */
        //   290: ldc             "\n UNION %s\n"
        //   292: iconst_1       
        //   293: anewarray       Ljava/lang/Object;
        //   296: dup            
        //   297: iconst_0       
        //   298: aload_0         /* this */
        //   299: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.requestContext:Lio/merak/awb/context/RequestContext;
        //   302: invokestatic    io/merak/awb/utils/config/AwbConfigs.useHiveUnionAll:(Lio/merak/awb/context/RequestContext;)Z
        //   305: ifeq            313
        //   308: ldc             " ALL "
        //   310: goto            315
        //   313: ldc             ""
        //   315: aastore        
        //   316: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   319: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   322: pop            
        //   323: aload_1         /* builder */
        //   324: ldc             "SELECT "
        //   326: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   329: pop            
        //   330: aload_0         /* this */
        //   331: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   334: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   337: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   342: aload_0         /* this */
        //   343: invokedynamic   BootstrapMethod #9, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   348: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   353: aload_0         /* this */
        //   354: aload_1         /* builder */
        //   355: aload           SCD2Granularity
        //   357: invokedynamic   BootstrapMethod #10, accept:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;Ljava/lang/StringBuilder;Ljava/lang/String;)Ljava/util/function/Consumer;
        //   362: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   367: aload_1         /* builder */
        //   368: aload_1         /* builder */
        //   369: invokevirtual   java/lang/StringBuilder.length:()I
        //   372: iconst_1       
        //   373: isub           
        //   374: aload_1         /* builder */
        //   375: invokevirtual   java/lang/StringBuilder.length:()I
        //   378: ldc             " "
        //   380: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   383: pop            
        //   384: aload_0         /* this */
        //   385: aload_1         /* builder */
        //   386: aload           joinTableName
        //   388: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.from:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   391: pop            
        //   392: aload_1         /* builder */
        //   393: ldc             " WHERE TD.ziw_row_id IS NOT NULL and TD.t_ziw_row_id IS NOT NULL "
        //   395: iconst_0       
        //   396: anewarray       Ljava/lang/Object;
        //   399: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   402: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   405: pop            
        //   406: aload_1         /* builder */
        //   407: ldc             "\n UNION \n"
        //   409: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   412: pop            
        //   413: aload_1         /* builder */
        //   414: ldc             "SELECT "
        //   416: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   419: pop            
        //   420: aload_0         /* this */
        //   421: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   424: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   427: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   432: aload_0         /* this */
        //   433: invokedynamic   BootstrapMethod #11, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   438: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   443: aload_0         /* this */
        //   444: aload_1         /* builder */
        //   445: aload           SCD2Granularity
        //   447: invokedynamic   BootstrapMethod #12, accept:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;Ljava/lang/StringBuilder;Ljava/lang/String;)Ljava/util/function/Consumer;
        //   452: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   457: aload_1         /* builder */
        //   458: aload_1         /* builder */
        //   459: invokevirtual   java/lang/StringBuilder.length:()I
        //   462: iconst_1       
        //   463: isub           
        //   464: aload_1         /* builder */
        //   465: invokevirtual   java/lang/StringBuilder.length:()I
        //   468: ldc             " "
        //   470: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   473: pop            
        //   474: aload_0         /* this */
        //   475: aload_1         /* builder */
        //   476: aload           joinTableName
        //   478: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.from:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   481: pop            
        //   482: aload_1         /* builder */
        //   483: ldc             " WHERE TD.ziw_row_id IS NOT NULL and TD.t_ziw_row_id IS NOT NULL%s%s"
        //   485: iconst_2       
        //   486: anewarray       Ljava/lang/Object;
        //   489: dup            
        //   490: iconst_0       
        //   491: aload_0         /* this */
        //   492: aload           SCD2Granularity
        //   494: invokespecial   io/merak/awb/exec/hive/translator/SqlTargetTranslator.getTimeSimilarityUDFExpression:(Ljava/lang/String;)Ljava/lang/String;
        //   497: aastore        
        //   498: dup            
        //   499: iconst_1       
        //   500: aload_0         /* this */
        //   501: iconst_1       
        //   502: invokespecial   io/merak/awb/exec/hive/translator/SqlTargetTranslator.isSCD2ColumnUpdated:(Z)Ljava/lang/String;
        //   505: aastore        
        //   506: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   509: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   512: pop            
        //   513: aload_1         /* builder */
        //   514: ldc             "\n UNION \n"
        //   516: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   519: pop            
        //   520: aload_1         /* builder */
        //   521: ldc             "SELECT "
        //   523: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   526: pop            
        //   527: aload_0         /* this */
        //   528: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   531: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   534: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   539: aload_0         /* this */
        //   540: invokedynamic   BootstrapMethod #13, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   545: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   550: aload_1         /* builder */
        //   551: invokedynamic   BootstrapMethod #14, accept:(Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //   556: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   561: aload_1         /* builder */
        //   562: aload_1         /* builder */
        //   563: invokevirtual   java/lang/StringBuilder.length:()I
        //   566: iconst_1       
        //   567: isub           
        //   568: aload_1         /* builder */
        //   569: invokevirtual   java/lang/StringBuilder.length:()I
        //   572: ldc             " "
        //   574: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   577: pop            
        //   578: aload_0         /* this */
        //   579: aload_1         /* builder */
        //   580: aload           joinTableName
        //   582: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.from:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   585: pop            
        //   586: aload_1         /* builder */
        //   587: ldc             " WHERE TD.ziw_row_id IS NULL"
        //   589: iconst_0       
        //   590: anewarray       Ljava/lang/Object;
        //   593: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   596: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   599: pop            
        //   600: aload_1         /* builder */
        //   601: ldc             "\n UNION \n"
        //   603: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   606: pop            
        //   607: aload_1         /* builder */
        //   608: ldc             "SELECT "
        //   610: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   613: pop            
        //   614: aload_0         /* this */
        //   615: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //   618: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //   621: invokeinterface java/util/List.stream:()Ljava/util/stream/Stream;
        //   626: aload_0         /* this */
        //   627: invokedynamic   BootstrapMethod #15, test:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;)Ljava/util/function/Predicate;
        //   632: invokeinterface java/util/stream/Stream.filter:(Ljava/util/function/Predicate;)Ljava/util/stream/Stream;
        //   637: aload_1         /* builder */
        //   638: invokedynamic   BootstrapMethod #16, accept:(Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //   643: invokeinterface java/util/stream/Stream.forEach:(Ljava/util/function/Consumer;)V
        //   648: aload_1         /* builder */
        //   649: aload_1         /* builder */
        //   650: invokevirtual   java/lang/StringBuilder.length:()I
        //   653: iconst_1       
        //   654: isub           
        //   655: aload_1         /* builder */
        //   656: invokevirtual   java/lang/StringBuilder.length:()I
        //   659: ldc             " "
        //   661: invokevirtual   java/lang/StringBuilder.replace:(IILjava/lang/String;)Ljava/lang/StringBuilder;
        //   664: pop            
        //   665: aload_0         /* this */
        //   666: aload_1         /* builder */
        //   667: aload_2         /* mainTable */
        //   668: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.from:(Ljava/lang/StringBuilder;Ljava/lang/String;)Lio/merak/awb/exec/hive/translator/SqlTranslator;
        //   671: pop            
        //   672: aload_1         /* builder */
        //   673: ldc             " WHERE ziw_active=false"
        //   675: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   678: pop            
        //   679: aload_0         /* this */
        //   680: aload_1         /* builder */
        //   681: invokevirtual   io/merak/awb/exec/hive/translator/SqlTargetTranslator.endWithoutComma:(Ljava/lang/StringBuilder;)V
        //   684: aload_1         /* builder */
        //   685: ldc             "INSERT OVERWRITE TABLE %s "
        //   687: iconst_1       
        //   688: anewarray       Ljava/lang/Object;
        //   691: dup            
        //   692: iconst_0       
        //   693: aload           mergedTable
        //   695: aastore        
        //   696: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   699: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   702: pop            
        //   703: aload_1         /* builder */
        //   704: ldc             "SELECT * FROM %s"
        //   706: iconst_1       
        //   707: anewarray       Ljava/lang/Object;
        //   710: dup            
        //   711: iconst_0       
        //   712: aload           unionTableName
        //   714: aastore        
        //   715: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //   718: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   721: pop            
        //   722: return         
        //    StackMapTable: 00 03 1E FF 01 1A 00 0B 07 00 F5 07 00 F6 07 00 D7 07 00 D7 07 00 D7 01 07 00 D7 07 00 D7 07 00 D7 07 00 D7 07 00 D7 00 05 07 00 F6 07 00 D7 07 00 F7 07 00 F7 01 FF 00 01 00 0B 07 00 F5 07 00 F6 07 00 D7 07 00 D7 07 00 D7 01 07 00 D7 07 00 D7 07 00 D7 07 00 D7 07 00 D7 00 06 07 00 F6 07 00 D7 07 00 F7 07 00 F7 01 07 00 F8
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
        //     8: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    11: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getOutputEntities:()Ljava/util/List;
        //    14: aload_0         /* this */
        //    15: aload_1         /* builder */
        //    16: invokedynamic   BootstrapMethod #17, accept:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //    21: invokeinterface java/util/List.forEach:(Ljava/util/function/Consumer;)V
        //    26: iload           includePrimaryPartitions
        //    28: ifeq            50
        //    31: aload_0         /* this */
        //    32: getfield        io/merak/awb/exec/hive/translator/SqlTargetTranslator.node:Lio/merak/awb/pipeline/dto/TargetNode;
        //    35: invokevirtual   io/merak/awb/pipeline/dto/TargetNode.getPrimaryPartitionColumns:()Ljava/util/List;
        //    38: aload_0         /* this */
        //    39: aload_1         /* builder */
        //    40: invokedynamic   BootstrapMethod #18, accept:(Lio/merak/awb/exec/hive/translator/SqlTargetTranslator;Ljava/lang/StringBuilder;)Ljava/util/function/Consumer;
        //    45: invokeinterface java/util/List.forEach:(Ljava/util/function/Consumer;)V
        //    50: aload_1         /* builder */
        //    51: aload_1         /* builder */
        //    52: invokevirtual   java/lang/StringBuilder.length:()I
        //    55: iconst_2       
        //    56: isub           
        //    57: invokevirtual   java/lang/StringBuilder.setLength:(I)V
        //    60: aload_1         /* builder */
        //    61: ldc             " FROM %s as T"
        //    63: iconst_1       
        //    64: anewarray       Ljava/lang/Object;
        //    67: dup            
        //    68: iconst_0       
        //    69: aload_2         /* mainTable */
        //    70: aastore        
        //    71: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //    74: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    77: pop            
        //    78: aload_1         /* builder */
        //    79: ldc             " FULL OUTER JOIN %s as D"
        //    81: iconst_1       
        //    82: anewarray       Ljava/lang/Object;
        //    85: dup            
        //    86: iconst_0       
        //    87: aload_3         /* deltaTable */
        //    88: aastore        
        //    89: invokestatic    java/lang/String.format:(Ljava/lang/String;[Ljava/lang/Object;)Ljava/lang/String;
        //    92: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //    95: pop            
        //    96: aload_1         /* builder */
        //    97: ldc             " ON (T.ziw_row_id = D.ziw_row_id)"
        //    99: invokevirtual   java/lang/StringBuilder.append:(Ljava/lang/String;)Ljava/lang/StringBuilder;
        //   102: pop            
        //   103: return         
        //    StackMapTable: 00 01 32
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
    
    private String getTimeSimilarityUDFExpression(final String scd2Granularity) {
        String exp = "";
        if (scd2Granularity.equalsIgnoreCase(TargetNode.SCD2GRANULARITY.SECOND.name())) {
            return exp;
        }
        exp = String.format(" and not (iwtime_similarity(TD.t_%s, TD.%s, '%s'))", "ZIW_TARGET_START_TIMESTAMP".toLowerCase(), "ZIW_TARGET_START_TIMESTAMP".toLowerCase(), scd2Granularity);
        return exp;
    }
    
    private String isSCD2ColumnUpdated(final boolean includeAnd) {
        final String exp = includeAnd ? " and " : "";
        final StringBuilder builder = new StringBuilder();
        final StringBuilder sb;
        this.node.getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).forEach(c -> {
            if (!this.node.getPrimaryPartitionColumns().contains(c) && !AwbUtil.isAuditColumn(c.getGivenName().toLowerCase()) && !this.node.isSCD1Column(c)) {
                sb.append(String.format("(if(TD.t_%s <=> TD.%s,false,true)) or ", c.getGivenName(), c.getGivenName(), c.getGivenName(), c.getGivenName()));
            }
            return;
        });
        if (builder.length() >= 4) {
            return String.format("%s(%s)", exp, builder.substring(0, builder.length() - 4));
        }
        builder.append("false ");
        return builder.toString();
    }
    
    private String isSCD1ColumnUpdated(final boolean includeAnd) {
        final String exp = includeAnd ? " and " : "";
        final StringBuilder builder = new StringBuilder();
        final StringBuilder sb;
        this.node.getOutputEntitiesUsed(this.requestContext.getProcessingContext().isValidationMode()).forEach(c -> {
            if (!this.node.getPrimaryPartitionColumns().contains(c) && !AwbUtil.isAuditColumn(c.getGivenName().toLowerCase()) && this.node.isSCD1Column(c)) {
                sb.append(String.format("(if(TD.t_%s <=> TD.%s,false,true)) or ", c.getGivenName(), c.getGivenName(), c.getGivenName(), c.getGivenName()));
            }
            return;
        });
        if (builder.length() >= 4) {
            return String.format("%s(%s)", exp, builder.substring(0, builder.length() - 4));
        }
        builder.append("false ");
        return builder.toString();
    }
    
    public String getOutputNameForProperty(final Entity input) {
        final Optional<Entity> output = this.node.getOutputEntitiesUsed(true).filter(out -> out.getReferencedEntity() != null && !out.getReferenceType().isDerivation() && out.getReferencedEntity().equals((Object)input)).findAny();
        return output.map((Function<? super Entity, ? extends String>)Entity::getOutputName).orElseGet(() -> String.format("`%s`", input.getGivenName()));
    }
    
    public TargetNode getTargetNode() {
        return this.node;
    }
}
