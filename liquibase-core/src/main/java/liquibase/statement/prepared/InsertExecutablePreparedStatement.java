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
 * Handles INSERT Execution
 */
public class InsertExecutablePreparedStatement extends
    AbstractPreparedStatement implements ExecutablePreparedStatement {

  private final Database database;
  private final InsertExecutablePreparedStatementChange change;
  private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

  private Info statement;
  private final boolean insert; //insert or update

  public InsertExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change) {
    this.database = database;
    this.change = change;
    this.insert = true;
  }

  protected InsertExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change, boolean insert) {
    this.database = database;
    this.change = change;
    this.insert = insert;
  }

  @Override
  public Info getStatement() {
    if (statement != null) {
      return statement;
    }

    // build the sql statement
    StringBuilder insertColumnSql = new StringBuilder("INSERT INTO ");
    StringBuilder insertValueSql = new StringBuilder("VALUES(");
    StringBuilder updateSql = new StringBuilder("UPDATE ");
    
    String tableName = database.escapeTableName(change.getCatalogName(),
        change.getSchemaName(), change.getTableName());
    insertColumnSql.append(tableName).append("(");
    if (!insert) {
      updateSql.append(tableName).append(" SET ");
    }

    for (ColumnConfig column : change.getColumns()) {
      if (database.supportsAutoIncrement()
          && Boolean.TRUE.equals(column.isAutoIncrement())) {
        continue;
      }
      String columnName = database.escapeColumnName(change.getCatalogName(),
          change.getSchemaName(), change.getTableName(), column.getName());
      insertColumnSql.append(columnName).append(", ");
      updateSql.append(columnName).append("=");

      Object valueObject = column.getValueObject();
      if (valueObject == null) {
        insertValueSql.append("null, ");
        updateSql.append("null, ");
      } else if (valueObject instanceof DatabaseFunction) {
        DatabaseFunction function = (DatabaseFunction)valueObject;
        insertValueSql.append(database.generateDatabaseFunctionValue(function)).append(", ");
        updateSql.append(database.generateDatabaseFunctionValue(function)).append(", ");
      } else {
        insertValueSql.append("?, ");
        updateSql.append("?, ");
        cols.add(column);
      }
    }

    insertColumnSql.deleteCharAt(insertColumnSql.lastIndexOf(" "));
    insertColumnSql.deleteCharAt(insertColumnSql.lastIndexOf(","));
    insertValueSql.deleteCharAt(insertValueSql.lastIndexOf(" "));
    insertValueSql.deleteCharAt(insertValueSql.lastIndexOf(","));
    updateSql.deleteCharAt(updateSql.lastIndexOf(" "));
    updateSql.deleteCharAt(updateSql.lastIndexOf(","));

    insertColumnSql.append(")");
    insertValueSql.append(")");
    
    insertColumnSql.append(" ").append(insertValueSql);
    if (!insert) {
      if (change instanceof UpdateExecutablePreparedStatementChange) {
        String whereClause = ((UpdateExecutablePreparedStatementChange) change)
            .getWhereClause();

        	updateSql.append(" where " + whereClause);
      } else {//insert
        updateSql.append(" where " + getPrimaryKeyClause(change.getPrimaryKey(),
            change.getColumns(), cols));
      }
    }

    String s = insert ? insertColumnSql.toString() : updateSql.toString();
    statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
        .getFilePath()));
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
