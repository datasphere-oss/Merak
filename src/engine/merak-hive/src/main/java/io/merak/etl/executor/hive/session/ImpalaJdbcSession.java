package io.merak.etl.executor.hive.session;

import merak.tools.security.authentication.*;
import merak.tools.utils.*;
import io.merak.etl.context.*;
import io.merak.etl.sdk.session.*;
import io.merak.etl.session.*;
import io.merak.etl.utils.config.*;
import merak.tools.ExceptionHandling.*;

import com.google.common.base.*;
import java.sql.*;
import java.util.*;

public class ImpalaJdbcSession extends JdbcExecutionSessionImpl
{
    public ImpalaJdbcSession(final RequestContext requestContext) {
        super(requestContext);
    }
    
    public Connection getConnection() throws SQLException {
        String impalaDriver = "";
        String connectionUrl = "";
        try {
            impalaDriver = AwbConfigs.getImpalaDriver();
            final String impalaURL = AwbConfigs.getImpalaURL();
            String connConfigs = AwbConfigs.getImpalaConnConfigs();
            if (AuthenticationUtil.isKerberosEnabled()) {
                connectionUrl = String.format("jdbc:%s/;%s", impalaURL, connConfigs);
            }
            else {
                final String impalaUser = AwbConfigs.getImpalaUser();
                final String impalaPassword = IWUtil.getImpalaPassword();
                connConfigs = String.format("user=%s;password=%s;%s", impalaUser, impalaPassword, connConfigs);
                connectionUrl = String.format("jdbc:%s/default;%s", impalaURL, connConfigs);
            }
        }
        catch (Exception e) {
            this.LOGGER.error("Unable to fetch Impala connection properties!");
            Throwables.propagate((Throwable)e);
        }
        try {
            Class.forName(impalaDriver);
            final Connection connection = DriverManager.getConnection(connectionUrl);
            this.preExecuteConfig(connection);
            return connection;
        }
        catch (Exception e) {
            this.LOGGER.error("Failed to Connect to Impala using: {}", (Object)connectionUrl);
            Throwables.propagate((Throwable)e);
            throw new UnsupportedOperationException("Unreachable!");
        }
    }
    
    protected void preExecuteConfig(final Connection connection) {
        this.preConfigStatements.ifPresent(stmts -> stmts.forEach(stmt -> {
            try {
                this.LOGGER.debug("Executing ImpalaJDBC preConfig statement: {}", (Object)stmt);
                connection.prepareStatement(stmt).execute(stmt);
            }
            catch (SQLException e) {
                this.LOGGER.error("SQLException: {}", (Object)AwbUtil.getErrorMessage((Throwable)e));
                Throwables.propagate((Throwable)new IWRunTimeException("{CONNECTION_CONFIG_ERROR}", (Throwable)e));
            }
        }));
    }
    
    public void executeQuery(final String stmt, final QueryProcessor processor) throws SQLException {
        if (Strings.isNullOrEmpty(stmt)) {
            return;
        }
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)stmt);
            try (final ResultSet resultSet = statement.executeQuery(stmt)) {
                processor.process(resultSet);
            }
        }
    }
    
    public void executeDDL(final List<String> ddl) throws SQLException {
        if (ddl == null || ddl.isEmpty()) {
            return;
        }
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            for (final String stmt : ddl) {
                this.LOGGER.debug("Executing statement: {}", (Object)stmt);
                statement.execute(stmt);
            }
        }
    }
    
    public void executeDDL(final String ddl) throws SQLException {
        if (Strings.isNullOrEmpty(ddl)) {
            return;
        }
        try (final Connection connection = this.getConnection();
             final Statement statement = connection.createStatement()) {
            this.LOGGER.debug("Executing statement: {}", (Object)ddl);
            statement.execute(ddl);
        }
    }
    
    public void shutdownSession() {
    }
}
