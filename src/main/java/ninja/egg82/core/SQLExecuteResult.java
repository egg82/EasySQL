package ninja.egg82.core;

public class SQLExecuteResult {
    private int recordsAffected;

    public SQLExecuteResult() { this.recordsAffected = -1; }

    public SQLExecuteResult(int recordsAffected) { this.recordsAffected = recordsAffected; }

    public int getRecordsAffected() { return recordsAffected; }
}
