package liquibase.statement.prepared;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.statement.DatabaseFunction;
import liquibase.statement.ExecutablePreparedStatement;

public class LoadExecutablePreparedStatement extends
    AbstractPreparedStatement
    implements ExecutablePreparedStatement {

  private final Database database;
  private final LoadExecutablePreparedStatementChange change;
  private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();
  private final String[] headers;
  private final String[] lines;

  private Info statement;
  private final int type; //1:insert, 2:update, 3:mysql on duplicate key update

  public LoadExecutablePreparedStatement(String[] headers, String[] lines,
      Database database, LoadExecutablePreparedStatementChange change) {
    this(headers, lines, database, change, 1);
  }

  public LoadExecutablePreparedStatement(String[] headers, String[] lines,
      Database database, LoadExecutablePreparedStatementChange change,
 int type) {
    this.database = database;
    this.headers = headers;
    this.lines = lines;
    this.change = change;
    this.type = type;
  }

  @Override
  public Info getStatement() {
    if (statement != null) {
      return statement;
    }

    // build the sql statement
    StringBuilder sql = new StringBuilder("INSERT INTO ");
    StringBuilder updateSql = new StringBuilder("UPDATE ");
    StringBuilder params = new StringBuilder("VALUES(");

    String tableName = database.escapeTableName(change.getCatalogName(),
        change.getSchemaName(), change.getTableName());
    sql.append(tableName).append("(");
    if (type == 2) {
      updateSql.append(tableName).append(" set ");
    }

    for (int i = 0; i < headers.length; i++) {
      Object value = lines[i];

      ColumnConfig headerColumn = change.getColumnConfig(i, headers[i]);
      if (headerColumn != null) {
        if ("skip".equalsIgnoreCase(headerColumn.getType())) {
          continue;
        }
        if (database.supportsAutoIncrement()
            && Boolean.TRUE.equals(headerColumn.isAutoIncrement())) {
          continue;
        }
        String columnName = database.escapeColumnName(change.getCatalogName(),
            change.getSchemaName(), change.getTableName(),
            headerColumn.getName());
        sql.append(columnName).append(", ");
        updateSql.append(columnName).append("=");

        ColumnConfig column = new ColumnConfig();
        column.setName(columnName);
        if (value != null) {
          if (headerColumn.getType().equalsIgnoreCase("BOOLEAN")) {
            column.setValueBoolean(Boolean.parseBoolean(value.toString()
                .toLowerCase()));
          } else if (headerColumn.getType().equalsIgnoreCase("NUMERIC")) {
            column.setValueNumeric(value.toString());
          } else if (headerColumn.getType().toUpperCase().contains("DATE")
              || headerColumn.getType().toUpperCase().contains("TIME")) {
            column.setValueDate(value.toString());
          } else if (headerColumn.getType().equalsIgnoreCase("STRING")) {
            column.setValue(value.toString());
          } else if (headerColumn.getType().equalsIgnoreCase("COMPUTED")) {
            column.setValueComputed(new DatabaseFunction(value.toString()));
          } else if (headerColumn.getType().equalsIgnoreCase("BLOB")) {
            column.setValueBlobFile(value.toString());
          } else if (headerColumn.getType().equalsIgnoreCase("CLOB")) {
            column.setValueClobFile(value.toString());
          } else {
            throw new UnexpectedLiquibaseException(
                "loadData type of "
                    + headerColumn.getType()
                    + " is not supported.  Please use BOOLEAN, NUMERIC, DATE, STRING, COMPUTED or SKIP");
          }
        }

        if (column.getValueObject() == null && column.getValueBlobFile() == null
            && column.getValueClobFile() == null) {
          params.append("null, ");
          updateSql.append("null, ");
        } else if (headerColumn.getDefaultValueComputed() != null) {
          String computedValue = headerColumn.getDefaultValueComputed().getValue();
          params.append(computedValue).append(", ");
          updateSql.append(computedValue).append(", ");
        } else {
          params.append("?, ");
          updateSql.append("?, ");
          cols.add(column);
        }
      }
    }

    sql.deleteCharAt(sql.lastIndexOf(" "));
    sql.deleteCharAt(sql.lastIndexOf(","));
    params.deleteCharAt(params.lastIndexOf(" "));
    params.deleteCharAt(params.lastIndexOf(","));
    updateSql.deleteCharAt(updateSql.lastIndexOf(" "));
    updateSql.deleteCharAt(updateSql.lastIndexOf(","));

    sql.append(") ");
    params.append(")");

    sql.append(params);
    if (type == 2) {
      updateSql.append(getPrimaryKeyWhereClause(change.getPrimaryKey(), cols,
          cols));
    }

    String s = sql.toString();
    if (type == 2 || type == 3) {
      s = updateSql.toString();
    }
    statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
        .getFilePath()));
    return statement;
  }

  @Override
  protected void setParameter(PreparedStatement stmt) throws DatabaseException {
    setParameter(stmt, 1);
  }

  protected void setParameter(PreparedStatement stmt, int paramStartAt)
      throws DatabaseException {
    try {
      setParameter(stmt, paramStartAt, cols, change.getChangeSet()
          .getFilePath(), change.getResourceAccessor());
    } catch (SQLException e) {
      throw new DatabaseException(e);
    }
  }

  public int getParameterSize() {
    return cols.size();
  }

}
