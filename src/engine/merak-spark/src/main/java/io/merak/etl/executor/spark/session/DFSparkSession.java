package io.merak.etl.executor.spark.session;

import io.merak.etl.sdk.session.*;
import org.apache.spark.sql.*;
import io.merak.etl.context.*;
import org.slf4j.*;
import io.merak.etl.job.dispatcher.*;
import io.merak.etl.job.dispatcher.handler.livy.*;

import org.apache.spark.scheduler.*;
import io.merak.platform.common.util.*;
import io.merak.etl.executor.spark.executor.*;
import io.merak.etl.executor.spark.progress.*;
import io.merak.etl.dispatcher.job.*;
import org.apache.spark.*;

import io.merak.etl.utils.config.*;
import java.util.*;
import com.google.common.collect.*;

public class DFSparkSession implements ExecutionEngineSession
{
    private final Logger LOGGER;
    private SparkSession spark;
    private RequestContext requestContext;
    private boolean isNativeJob;
    private DFSparkSession builder;
    private static Map<RequestContext, DFSparkSession> builderMap;
    
    private DFSparkSession(final RequestContext reqCtx) {
        this.LOGGER = LoggerFactory.getLogger((Class)DFSparkSession.class);
        this.isNativeJob = true;
        this.builder = null;
        this.requestContext = reqCtx;
    }
    
    public static synchronized DFSparkSession getInstance(final RequestContext reqCtx) {
        if (DFSparkSession.builderMap.get(reqCtx) == null) {
            DFSparkSession.builderMap.put(reqCtx, new DFSparkSession(reqCtx));
        }
        return DFSparkSession.builderMap.get(reqCtx);
    }
    
    public synchronized SparkSession getSpark() {
        this.LOGGER.info("requestContext.getConfig(\"shouldAddListener\"): {}", (Object)this.requestContext.getConfig("shouldAddListener"));
        try {
            if (this.spark == null || this.spark.sparkContext().isStopped()) {
                if (JobType.getJobTypeFrom(AwbUtil.getDispatcherJobType(this.requestContext)) == JobType.LIVY_JOB) {
                    this.spark = IWLivyJob.getSparkSession();
                    this.isNativeJob = false;
                }
                else if (JobType.getJobTypeFrom(AwbUtil.getDispatcherJobType(this.requestContext)) == JobType.DATABRICKS_JOB) {
                    this.spark = SparkSession.builder().master("local").getOrCreate();
                    if (this.requestContext.getConfig("shouldAddListener") != null) {
                        this.LOGGER.info("adding spark listener");
                        this.spark.sparkContext().addSparkListener((SparkListenerInterface)new PipelineJobProgressListener(this.requestContext.getJobId()));
                    }
                    this.isNativeJob = false;
                }
                else {
                    this.initSparkSession();
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            this.LOGGER.error("Exception : {}", (Object)e.getMessage());
        }
        this.LOGGER.info("spark id: {}", (Object)this.spark.toString());
        return this.spark;
    }
    
    private void initSecurity() {
        try {
            KerberosUtils.refreshKerberos();
        }
        catch (Exception e) {
            e.printStackTrace();
            this.LOGGER.warn("Failed while initializing security for spark");
        }
    }
    
    private void initSparkSession() {
        this.initSecurity();
        this.LOGGER.debug("Creating new spark session ");
        final SparkConf sparkConf = PipelineUtils.getSparkConf(this.requestContext);
        if (PipelineUtils.getDispatcherJobType(this.requestContext) == JobType.SPARK) {
            this.spark = SparkSession.builder().enableHiveSupport().getOrCreate();
        }
        else {
            this.spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate();
        }
        this.addShutDownHook();
        this.LOGGER.debug("Successfully created spark session for DF:");
    }
    
    private void addShutDownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> this.shutdown()));
    }
    
    public void shutdown() {
        try {
            if (this.spark != null) {
                this.LOGGER.debug("Stopping spark session..");
                DataFrameCache.clearCaches();
                if (this.isNativeJob) {
                    this.LOGGER.debug("Stopping spark session for native job");
                    this.spark.stop();
                }
            }
        }
        catch (Exception e) {
            this.LOGGER.error("Error stopping spark ", (Throwable)e);
        }
    }
    
    public void shutDownDependentSession() {
        if (AwbConfigs.useH2OasMLEngine(this.requestContext)) {}
        this.requestContext = null;
    }
    
    public void refreshTableCache(final String tableName) {
        this.spark.catalog().refreshTable(tableName);
    }
    
    public void executeStatements(final List<String> stmts) {
        for (final String stmt : stmts) {
            this.executeStatement(stmt);
        }
    }
    
    public void executeStatement(final String stmt) {
        this.LOGGER.info("Executing stmt {}", (Object)stmt);
        this.spark.sql(stmt);
    }
    
    public void shutdownSession() {
        this.shutdown();
        this.shutDownDependentSession();
    }
    
    static {
        DFSparkSession.builderMap = (Map<RequestContext, DFSparkSession>)Maps.newHashMap();
    }
}
