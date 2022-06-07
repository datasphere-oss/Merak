package io.merak.etl.executor.hive.session;

import merak.tools.ExceptionHandling.*;
import com.google.common.base.*;
import merak.tools.hadoop.mapreduce.*;
import merak.tools.hive.*;
import java.io.*;
import java.sql.*;
import java.util.*;

import io.merak.etl.context.*;
import io.merak.etl.executor.hive.progress.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.session.*;
import io.merak.etl.utils.config.*;

public class HiveJdbcSession extends JdbcExecutionSessionImpl
{
    public HiveJdbcSession(final RequestContext requestContext) {
        super(requestContext);
    }
    
    protected void preExecuteConfig(final Connection connection) {
        this.preConfigStatements.ifPresent(stmts -> stmts.forEach(stmt -> {
            try {
                this.LOGGER.debug("Executing HiveJDBC preConfig statement: {}", (Object)stmt);
                connection.prepareStatement(stmt).execute(stmt);
            }
            catch (SQLException e) {
                this.LOGGER.error("SQLException: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                Throwables.propagate((Throwable)new IWRunTimeException("{CONNECTION_CONFIG_ERROR}", (Throwable)e));
            }
        }));
    }
    
    public Connection getConnection() throws SQLException {
        try {
            final Connection connection = HiveUtils.getConnection(this.jobId, IWJob.JOB_TYPE.AWB_INTERACTIVE.name());
            this.executeConfigQueries(connection);
            this.preExecuteConfig(connection);
            return connection;
        }
        catch (IOException e) {
            Throwables.propagate((Throwable)e);
            throw new UnsupportedOperationException("Unreachable!");
        }
    }
    
    public void executeQuery(final String stmt, final QueryProcessor processor) throws SQLException {
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)stmt);
            try (final ResultSet resultSet = statement.executeQuery(stmt);
                 final HiveQueryProgressFinder finder = new HiveQueryProgressFinder(statement, stmt)) {
                finder.logHive();
                processor.process(resultSet);
            }
        }
    }
    
    public void executeDDL(final List<String> ddl) throws SQLException {
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            for (final String stmt : ddl) {
                this.LOGGER.debug("Executing statement: {}", (Object)stmt);
                try (final HiveQueryProgressFinder finder = new HiveQueryProgressFinder(statement, stmt)) {
                    finder.logHive();
                    statement.execute(stmt);
                }
            }
        }
    }
    
    public void executeDDL(final String ddl) throws SQLException {
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)ddl);
            try (final HiveQueryProgressFinder finder = new HiveQueryProgressFinder(statement, ddl)) {
                finder.logHive();
                statement.execute(ddl);
            }
        }
    }
    
    private void executeConfigQueries(final Connection connection) {
        this.LOGGER.debug("Executing Config Queries ");
        final List<String> configsForPipeline = (List<String>)AwbConfigs.getHiveConfigOverwrite(this.requestContext, this.requestContext.getJobType());
        String stmt;
        configsForPipeline.forEach(config -> {
            try {
                if (config != null && !config.isEmpty()) {
                    stmt = String.format("SET %s", config);
                    this.LOGGER.debug("Executing Config {}", (Object)stmt);
                    connection.prepareStatement(stmt).execute();
                }
            }
            catch (Exception e) {
                this.LOGGER.error("Exception: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                Throwables.propagate((Throwable)new IWRunTimeException("{PIPELINE_CONFIGURATION_ERROR}", (Throwable)e));
            }
        });
    }
    
    public void shutdownSession() {
    }
}
