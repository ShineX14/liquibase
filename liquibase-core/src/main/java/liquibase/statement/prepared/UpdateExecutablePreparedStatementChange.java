package liquibase.statement.prepared;


public interface UpdateExecutablePreparedStatementChange extends
    InsertExecutablePreparedStatementChange {

  String getWhereClause();

}
