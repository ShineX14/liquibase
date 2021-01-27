package liquibase.statement.prepared;


public interface DeleteExecutablePreparedStatementChange extends
    InsertExecutablePreparedStatementChange {

  String getWhereClause();

}
