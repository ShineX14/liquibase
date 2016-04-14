package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.change.core.InsertDataChange;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

public class LoadExecutablePreparedStatement extends AbstractLoadPreparedStatement {

  private final ExecutablePreparedStatement statement;

  public LoadExecutablePreparedStatement(String[] headers, String[] lines,
      Database database, LoadExecutablePreparedStatementChange change) {
    InsertDataChange insertChange = new InsertDataChange();
    copyFromLoadExcutablePreparedStatementChange(insertChange, change, headers, lines);
    statement = new InsertExecutablePreparedStatement(database, insertChange);
  }

  @Override
  public Info getStatement() {
    return statement.getStatement();
  }

  @Override
  public void setParameter(PreparedStatement stmt) throws DatabaseException {
    statement.setParameter(stmt);
  }

}
