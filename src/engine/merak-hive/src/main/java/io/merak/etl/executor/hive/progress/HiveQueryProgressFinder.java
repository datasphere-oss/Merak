package io.merak.etl.executor.hive.progress;

import java.util.concurrent.atomic.*;
import java.util.*;
import org.apache.hive.jdbc.*;
import java.sql.*;
import org.slf4j.*;

import io.merak.etl.utils.config.*;

public class HiveQueryProgressFinder implements AutoCloseable
{
    private static final Logger LOGGER;
    private HiveStatement hiveStatement;
    private String sql;
    private volatile AtomicBoolean queryCompleted;
    private static final long POLLING_FREQ;
    private String uuid;
    
    public HiveQueryProgressFinder(final Statement hiveStatement, final String sql) {
        this.queryCompleted = new AtomicBoolean(false);
        this.hiveStatement = (HiveStatement)hiveStatement;
        this.sql = sql.substring(0, Math.min(100, sql.length())) + " .... ";
        this.uuid = UUID.randomUUID().toString();
        this.logInfoUUID("starting hive logs for query {}", this.sql);
    }
    
    public void complete() {
        this.logTraceUUID(" on complete: hive statement has more logs {} query completed {}", this.hiveStatement.hasMoreLogs(), this.queryCompleted.get());
        this.queryCompleted.set(true);
        try {
            final List<String> logList = (List<String>)this.hiveStatement.getQueryLog();
            logList.forEach(i -> this.logInfoUUID(i, new Object[0]));
        }
        catch (Exception e) {
            this.logWarnUUID("exception while running sql {}", e);
        }
        this.logInfoUUID("completed hive logs for query {}", this.sql);
    }
    
    public void logHive() {
        List logList;
        final Thread thread = new Thread(() -> {
            this.logTraceUUID("hive statement hasmore logs {} queryComplete {}", this.hiveStatement.hasMoreLogs(), this.queryCompleted.get());
            while (!this.queryCompleted.get()) {
                try {
                    logList = this.hiveStatement.getQueryLog();
                    logList.forEach(i -> this.logInfoUUID(i, new Object[0]));
                }
                catch (ClosedOrCancelledStatementException e1) {
                    this.logWarnUUID("cancelled exception while running sql", e1);
                    this.complete();
                }
                catch (SQLException e2) {
                    this.logWarnUUID("sqlexception running sql", e2);
                    this.complete();
                }
                try {
                    this.logTraceUUID("sleeping ... for {} ms", HiveQueryProgressFinder.POLLING_FREQ);
                    Thread.sleep(HiveQueryProgressFinder.POLLING_FREQ);
                }
                catch (InterruptedException ex) {}
            }
            this.logTraceUUID("done waiting for logs", new Object[0]);
            return;
        }, "HiveQueryProgressFinderThread");
        thread.start();
    }
    
    private void logInfoUUID(final String format, final Object... argArray) {
        HiveQueryProgressFinder.LOGGER.info(String.format("UUID:%s  %s", this.uuid, format), argArray);
    }
    
    private void logWarnUUID(final String format, final Object... argArray) {
        HiveQueryProgressFinder.LOGGER.warn(String.format("UUID:%s  %s", this.uuid, format), argArray);
    }
    
    private void logTraceUUID(final String format, final Object... argArray) {
        HiveQueryProgressFinder.LOGGER.trace(String.format("UUID:%s  %s", this.uuid, format), argArray);
    }
    
    @Override
    public void close() {
        this.complete();
    }
    
    static {
        LOGGER = LoggerFactory.getLogger((Class)HiveQueryProgressFinder.class);
        POLLING_FREQ = AwbConfigs.getHiveLoggingFrequency();
    }
}
