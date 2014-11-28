package liquibase.statement.prepared;

import liquibase.database.Database;

/**
 * Handles INSERT Execution
 */
public class UpdateExecutablePreparedStatement extends
    InsertExecutablePreparedStatement {

  public UpdateExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change) {
    super(database, change, false);
  }

}
