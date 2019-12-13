package ninja.egg82.core;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedParameterCallableStatement implements AutoCloseable {
    private static final Pattern FIND_PATTERN = Pattern.compile("(?<!')(:[\\w]*)(?!')");

    private CallableStatement statement;
    private List<String> fields = new ArrayList<>();

    public NamedParameterCallableStatement(Connection conn, String statementWithNames) throws SQLException {
        Matcher matcher = FIND_PATTERN.matcher(statementWithNames);
        while (matcher.find()) {
            fields.add(matcher.group().substring(1));
        }
        statement = conn.prepareCall(statementWithNames.replaceAll(FIND_PATTERN.pattern(), "?"));
    }

    public PreparedStatement getPreparedStatement() { return statement; }

    public void close() throws SQLException { statement.close(); }

    public void setObject(String name, Object value) throws SQLException {
        statement.setObject(getIndex(name), value);
    }

    public void addBatch() throws SQLException {
        statement.addBatch();
    }

    private int getIndex(String name) { return fields.indexOf(name) + 1; }
}
