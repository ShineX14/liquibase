package liquibase.statement.prepared;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.ExecutablePreparedStatement;

/**
 * Handles DELETE Execution
 */
public class DeleteExecutablePreparedStatement extends
    AbstractPreparedStatement implements ExecutablePreparedStatement {

  private final DeleteExecutablePreparedStatementChange change;
  private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

  private Info statement;

  public DeleteExecutablePreparedStatement(Database database,
      DeleteExecutablePreparedStatementChange change) {
    this.database = database;
    this.change = change;
  }

  protected DeleteExecutablePreparedStatement(Database database,
      DeleteExecutablePreparedStatementChange change, boolean insert) {
    this.database = database;
    this.change = change;
  }

  @Override
  public Info getStatement() {
    if (statement != null) {
      return statement;
    }

    // build the sql statement
    StringBuilder deleteSql = new StringBuilder("DELETE FROM ");

    String tableName = database.escapeTableName(change.getCatalogName(),
        change.getSchemaName(), change.getTableName());
    deleteSql.append(tableName).append(" WHERE ");

    for (ColumnConfig column : change.getColumns()) {
      if (database.supportsAutoIncrement()
          && Boolean.TRUE.equals(column.isAutoIncrement())) {
        continue;
      }
      String columnName = database.escapeColumnName(change.getCatalogName(),
          change.getSchemaName(), change.getTableName(), column.getName());
      deleteSql.append(columnName);

      Object valueObject = column.getValueObject();
      if (valueObject == null) {
        deleteSql.append(" is null, ");
      } else if (valueObject instanceof DatabaseFunction) {
        DatabaseFunction function = (DatabaseFunction)valueObject;
        deleteSql.append("=").append(database.generateDatabaseFunctionValue(function)).append(", ");
      } else {
        deleteSql.append("=?, ");
        cols.add(column);
      }
    }

    deleteSql.deleteCharAt(deleteSql.lastIndexOf(" "));
    deleteSql.deleteCharAt(deleteSql.lastIndexOf(","));

    statement = new Info(deleteSql.toString(), cols, getParameters(cols, change.getChangeSet().getFilePath()));
    return statement;
  }

  @Override
  public void setParameter(PreparedStatement stmt) throws DatabaseException {
    try {
      setParameter(stmt, 1, cols, change.getChangeSet().getChangeLog()
          .getPhysicalFilePath(), change.getResourceAccessor());
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  public int getParameterSize() {
    return cols.size();
  }
}
