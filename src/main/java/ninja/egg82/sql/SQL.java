package ninja.egg82.sql;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Properties;

import ninja.egg82.core.NamedParameterCallableStatement;
import ninja.egg82.core.NamedParameterStatement;
import ninja.egg82.core.SQLExecuteResult;
import ninja.egg82.core.SQLQueryResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

public class SQL implements AutoCloseable {
    private HikariDataSource source;

    public SQL(HikariConfig config) { source = new HikariDataSource(config); }

    public SQL(Properties properties) { source = new HikariDataSource(new HikariConfig(properties)); }

    public SQL(String propertiesFile) { source = new HikariDataSource(new HikariConfig(propertiesFile)); }

    public SQL(String connectionString, String user, String pass) {
        source = new HikariDataSource();
        source.setJdbcUrl(connectionString);
        source.setUsername(user);
        source.setPassword(pass);
        source.setAutoCommit(true);
    }

    public HikariDataSource getRawSource() { return source; }

    public void close() { source.close(); }

    public boolean isClosed() { return source.isClosed(); }

    public boolean isRunning() { return source.isRunning(); }

    public boolean tableExists(String schemaPattern, String tablePattern) throws SQLException {
        try (Connection connection = source.getConnection(); ResultSet results = connection.getMetaData().getTables(null, schemaPattern, tablePattern, null)) {
            return results.next();
        }
    }

    public CompletableFuture<Boolean> tableExistsAsync(String schemaPattern, String tablePattern) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return tableExists(schemaPattern, tablePattern);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLQueryResult query(String q, Object... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            SQLQueryResult result = query(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLQueryResult query(String q, Map<String, Object> namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q)) {
            if (namedParams != null) {
                for (Map.Entry<String, Object> kvp : namedParams.entrySet()) {
                    statement.setObject(kvp.getKey(), kvp.getValue());
                }
            }

            SQLQueryResult result = query(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLQueryResult> queryAsync(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLQueryResult> queryAsync(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return query(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLExecuteResult execute(String q, Object... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            SQLExecuteResult result = execute(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLExecuteResult execute(String q, Map<String, Object> namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q, Statement.RETURN_GENERATED_KEYS)) {
            if (namedParams != null) {
                for (Map.Entry<String, Object> kvp : namedParams.entrySet()) {
                    statement.setObject(kvp.getKey(), kvp.getValue());
                }
            }

            SQLExecuteResult result = execute(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLExecuteResult> executeAsync(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return execute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLExecuteResult> executeAsync(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return execute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLExecuteResult[] batchExecute(String q, Object[]... params) throws SQLException {
        try (Connection connection = source.getConnection(); PreparedStatement statement = connection.prepareStatement(q, Statement.RETURN_GENERATED_KEYS)) {
            if (params != null) {
                for (Object[] p : params) {
                    if (p != null) {
                        for (int i = 0; i < p.length; i++) {
                            statement.setObject(i + 1, p[i]);
                        }
                        statement.addBatch();
                    }
                }
            }

            SQLExecuteResult[] result = executeBatch(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLExecuteResult[] batchExecute(String q, Map<String, Object>... namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterStatement statement = new NamedParameterStatement(connection, q, Statement.RETURN_GENERATED_KEYS)) {
            if (namedParams != null) {
                for (Map<String, Object> p : namedParams) {
                    if (p != null) {
                        for (Map.Entry<String, Object> kvp : p.entrySet()) {
                            statement.setObject(kvp.getKey(), kvp.getValue());
                        }
                        statement.addBatch();
                    }
                }
            }

            SQLExecuteResult[] result = executeBatch(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecuteAsync(String q, Object[]... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return batchExecute(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLExecuteResult[]> batchExecuteAsync(String q, Map<String, Object>... namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return batchExecute(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public SQLQueryResult call(String q, Object... params) throws SQLException {
        try (Connection connection = source.getConnection(); CallableStatement statement = connection.prepareCall(q)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    statement.setObject(i + 1, params[i]);
                }
            }

            SQLQueryResult result = query(statement);
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public SQLQueryResult call(String q, Map<String, Object> namedParams) throws SQLException {
        try (Connection connection = source.getConnection(); NamedParameterCallableStatement statement = new NamedParameterCallableStatement(connection, q)) {
            if (namedParams != null) {
                for (Map.Entry<String, Object> kvp : namedParams.entrySet()) {
                    statement.setObject(kvp.getKey(), kvp.getValue());
                }
            }

            SQLQueryResult result = query(statement.getPreparedStatement());
            if (!source.isAutoCommit()) {
                connection.commit();
            }
            return result;
        }
    }

    public CompletableFuture<SQLQueryResult> calAsync(String q, Object... params) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return call(q, params);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    public CompletableFuture<SQLQueryResult> callAsync(String q, Map<String, Object> namedParams) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return call(q, namedParams);
            } catch (SQLException ex) {
                throw new CompletionException(ex);
            }
        });
    }

    private SQLQueryResult query(PreparedStatement statement) throws SQLException {
        boolean hasResults = statement.execute();

        if (!hasResults) {
            return new SQLQueryResult();
        }

        List<String> columns = new ArrayList<>();
        List<Object[]> rows = new ArrayList<>();

        try (ResultSet results = statement.getResultSet()) {
            ResultSetMetaData meta = results.getMetaData();

            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }

            collectRows(results, rows, columns.size());
        }

        while (statement.getMoreResults()) {
            try (ResultSet results = statement.getResultSet()) {
                collectRows(results, rows, columns.size());
            }
        }

        return new SQLQueryResult(columns.toArray(new String[0]), rows.toArray(new Object[0][]));
    }

    private void collectRows(ResultSet results, List<Object[]> rows, int columnCount) throws SQLException {
        while (results.next()) {
            Object[] tVals = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                tVals[i] = results.getObject(i + 1);
            }
            rows.add(tVals);
        }
    }

    private SQLExecuteResult execute(PreparedStatement statement) throws SQLException {
        boolean hasResults = statement.execute();

        List<String> columns = new ArrayList<>();
        List<Object[]> keys = new ArrayList<>();

        try (ResultSet results = statement.getGeneratedKeys()) {
            ResultSetMetaData meta = results.getMetaData();

            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }

            collectRows(results, keys, columns.size());
        }

        try (ResultSet results = statement.getGeneratedKeys()) {
            collectRows(results, keys, columns.size());
        }

        if (hasResults) {
            return new SQLExecuteResult(-1, columns.toArray(new String[0]), keys.isEmpty() ? new Object[0] : keys.get(0));
        }

        return new SQLExecuteResult(statement.getUpdateCount(), columns.toArray(new String[0]), keys.isEmpty() ? new Object[0] : keys.get(0));
    }

    private SQLExecuteResult[] executeBatch(PreparedStatement statement) throws SQLException {
        int[] updates = statement.executeBatch();
        statement.clearBatch();

        List<String> columns = new ArrayList<>();
        List<Object[]> keys = new ArrayList<>();

        try (ResultSet results = statement.getGeneratedKeys()) {
            ResultSetMetaData meta = results.getMetaData();

            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columns.add(meta.getColumnName(i));
            }

            collectRows(results, keys, columns.size());
        }

        try (ResultSet results = statement.getGeneratedKeys()) {
            collectRows(results, keys, columns.size());
        }

        String[] columnsArray = columns.toArray(new String[0]);

        SQLExecuteResult[] retVal = new SQLExecuteResult[updates.length];
        for (int i = 0; i < updates.length; i++) {
            retVal[i] = new SQLExecuteResult(updates[i], columnsArray, keys.size() > i ? keys.get(i) : new Object[0]);
        }

        return retVal;
    }
}
