package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.change.core.InsertUpdateDataChange;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;

public class LoadUpdateExecutablePreparedStatement extends AbstractLoadPreparedStatement {

  private final InsertUpdateExecutablePreparedStatement insertUpdate;

  public LoadUpdateExecutablePreparedStatement(String[] headers, String[] lines, Database database,
      LoadExecutablePreparedStatementChange change) {
    InsertUpdateDataChange insertUpdateChange = new InsertUpdateDataChange();
    copyFromLoadExcutablePreparedStatementChange(insertUpdateChange, change, headers, lines);
    insertUpdateChange.setPrimaryKey(change.getPrimaryKey());
    this.insertUpdate = new InsertUpdateExecutablePreparedStatement(database, insertUpdateChange);
  }

  @Override
  public Info getStatement() {
    return insertUpdate.getStatement();
  }

  @Override
  public void setParameter(PreparedStatement stmt) throws DatabaseException {
    insertUpdate.setParameter(stmt);
  }

}
