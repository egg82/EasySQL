package ninja.egg82.core;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NamedParameterStatement implements AutoCloseable {
    private static final Pattern FIND_PATTERN = Pattern.compile("(?<!')(:[\\w]*)(?!')");

    private PreparedStatement statement;
    private List<String> fields = new ArrayList<>();

    public NamedParameterStatement(Connection conn, String statementWithNames) throws SQLException { this(conn, statementWithNames, Statement.NO_GENERATED_KEYS); }

    public NamedParameterStatement(Connection conn, String statementWithNames, int keys) throws SQLException {
        Matcher matcher = FIND_PATTERN.matcher(statementWithNames);
        while (matcher.find()) {
            fields.add(matcher.group().substring(1));
        }
        statement = conn.prepareStatement(statementWithNames.replaceAll(FIND_PATTERN.pattern(), "?"), keys);
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
