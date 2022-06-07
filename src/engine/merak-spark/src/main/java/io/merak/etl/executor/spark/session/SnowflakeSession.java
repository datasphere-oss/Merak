package io.merak.etl.executor.spark.session;

import io.merak.etl.session.*;
import io.merak.etl.executor.spark.translator.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.context.*;

import com.google.common.base.*;
import io.merak.etl.sdk.session.*;
import java.sql.*;
import java.util.*;

public class SnowflakeSession extends JdbcExecutionSessionImpl
{
    SnowFlakeTargetNode node;
    
    public SnowflakeSession(final RequestContext requestContext, final SnowFlakeTargetNode node) {
        super(requestContext);
        this.node = node;
    }
    
    public Connection getConnection() throws SQLException {
        try {
            this.LOGGER.info("node db name {}", (Object)this.node.getDatabaseName());
            this.node.getClass();
            Class.forName("net.snowflake.client.jdbc.SnowflakeDriver");
            final String url = String.format("jdbc:snowflake://%s", this.node.getURL());
            final Properties jdbcProperties = SnowFlakeTargetTranslator.getJdbcProperties(this.node);
            final Connection conn = DriverManager.getConnection(url, jdbcProperties);
            return conn;
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
            Throwables.propagate((Throwable)e);
            return null;
        }
    }
    
    public void executeQuery(final String stmt, final QueryProcessor processor) throws SQLException {
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)stmt);
            try (final ResultSet resultSet = statement.executeQuery(stmt)) {
                processor.process(resultSet);
            }
        }
    }
    
    public ResultSet executeQuery(final String stmt) throws SQLException {
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)stmt);
            try (final ResultSet resultSet = statement.executeQuery(stmt)) {
                return resultSet;
            }
        }
    }
    
    protected void executeDDL(final String stmt) throws SQLException {
    }
    
    protected void executeDDL(final List<String> stmts) throws SQLException {
    }
    
    protected void preExecuteConfig(final Connection connection) {
    }
    
    public void shutdownSession() {
    }
}
