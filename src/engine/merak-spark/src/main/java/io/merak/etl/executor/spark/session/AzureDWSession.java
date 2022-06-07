package io.merak.etl.executor.spark.session;

import io.merak.etl.session.*;
import io.merak.etl.pipeline.dto.*;
import io.merak.etl.context.*;
import io.merak.etl.sdk.session.*;
import java.sql.*;
import java.util.*;

public class AzureDWSession extends JdbcExecutionSessionImpl
{
    AzureDWNode node;
    
    public AzureDWSession(final RequestContext requestContext, final AzureDWNode node) {
        super(requestContext);
        this.node = node;
    }
    
    public Connection getConnection() throws SQLException {
        return null;
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
