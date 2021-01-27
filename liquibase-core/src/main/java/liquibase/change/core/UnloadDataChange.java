package liquibase.change.core;

import liquibase.change.ChangeMetaData;
import liquibase.change.ChangeStatus;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.RollbackImpossibleException;
import liquibase.statement.ExecutablePreparedStatement;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.InsertStatement;
import liquibase.statement.prepared.UnloadExecutablePreparedStatement;
import liquibase.statement.prepared.UnloadExecutablePreparedStatementChange;


@DatabaseChange(name = "unloadData", description = "Delete data from a CSV file",
priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "table", since = "3.3.7")
public class UnloadDataChange extends LoadUpdateDataChange implements UnloadExecutablePreparedStatementChange {

  @Override
  protected ExecutablePreparedStatement createExecutablePreparedStatement(
      String[] headers, String[] line, Database database) {
    return new UnloadExecutablePreparedStatement(headers, line, database, this);
  }

  @Override
  protected InsertStatement createStatement(String catalogName, String schemaName, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SqlStatement[] generateRollbackStatements(Database database) throws RollbackImpossibleException {
    throw new RollbackImpossibleException();
  }

  @Override
  public String getPrimaryKey() {
    return super.getPrimaryKey();
  }

  @Override
  public String getConfirmationMessage() {
    return "Data deleted from " + getTableName();
  }

  @Override
  public ChangeStatus checkStatus(Database database) {
    return new ChangeStatus().unknown("Cannot check unloadData status");
  }

}
