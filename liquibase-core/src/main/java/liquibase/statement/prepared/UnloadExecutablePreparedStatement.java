package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.change.ColumnConfig;
import liquibase.change.core.DeleteDataChange;
import liquibase.change.core.UnloadDataChange;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;

public class UnloadExecutablePreparedStatement extends AbstractLoadPreparedStatement {

  private final DeleteExecutablePreparedStatement delete;

  public UnloadExecutablePreparedStatement(String[] headers, String[] lines, Database database,
      UnloadDataChange change) {
    DeleteDataChange deleteChange = new DeleteDataChange();
    copyFromLoadExcutablePreparedStatementChange(deleteChange, change, headers, lines);
    this.delete = new DeleteExecutablePreparedStatement(database, deleteChange);
  }

  protected void copyFromLoadExcutablePreparedStatementChange(DeleteDataChange change,
      LoadExecutablePreparedStatementChange orgChange, String[] headers, String[] lines) {
    change.setCatalogName(orgChange.getCatalogName());
    change.setSchemaName(orgChange.getSchemaName());
    change.setChangeSet(orgChange.getChangeSet());
    change.setResourceAccessor(orgChange.getResourceAccessor());
    change.setTableName(orgChange.getTableName());
    for (int i=0; i<headers.length; i++) {
      ColumnConfig headerColumn = orgChange.getColumnConfig(i, headers[i]);
      if (headerColumn == null) {
        continue;
      }

      ColumnConfig valueColumn = getColumnConfig(headerColumn, lines[i]);
      change.addWhereParam(valueColumn);
    }
  }

  @Override
  public Info getStatement() {
    return delete.getStatement();
  }

  @Override
  public void setParameter(PreparedStatement stmt) throws DatabaseException {
    delete.setParameter(stmt);
  }

}
