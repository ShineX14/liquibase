package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

public class LoadUpdateExecutablePreparedStatement extends
    AbstractPreparedStatement implements ExecutablePreparedStatement {

  private final Database database;
  private final LoadExecutablePreparedStatement insert;
  private final LoadExecutablePreparedStatement update;
  private final LoadExecutablePreparedStatementChange change;
  private Info statement;

  public LoadUpdateExecutablePreparedStatement(String[] headers,
      String[] lines, Database database,
      LoadExecutablePreparedStatementChange change) {
    this.database = database;
    this.insert = new LoadExecutablePreparedStatement(headers, lines, database,
        change);
    if (database instanceof MySQLDatabase) {
      this.update = new LoadExecutablePreparedStatement(headers, lines,
          database, change, 3);
    } else {
      this.update = new LoadExecutablePreparedStatement(headers, lines,
          database, change, 2);
    }
    this.change = change;
  }

  @Override
  public Info getStatement() {
    if (statement != null) {
      return statement;
    }

    StringBuilder sql = null;
    if (database instanceof OracleDatabase) {
      sql = getOracleSql();
    } else if (database instanceof MySQLDatabase) {
      sql = getMysqlSql();
    } else {
      throw new IllegalArgumentException(database
          + " is not expected in batch mode");
    }

    statement = new Info(sql.toString(), update.getStatement().columns,
        getParameters(update.getStatement().columns, change.getChangeSet()
            .getFilePath()));
    return statement;
  }

  private StringBuilder getOracleSql() {
    StringBuilder sql = new StringBuilder();
    sql.append("BEGIN\n  ");
    sql.append(update.getStatement().sql).append(";\n  ");
    sql.append("IF SQL%ROWCOUNT=0 THEN\n    ");
    sql.append(insert.getStatement().sql).append(";\n  ");
    sql.append("END IF;\n");
    sql.append("END;");
    return sql;
  }

  private StringBuilder getMysqlSql() {
    StringBuilder sql = new StringBuilder();
    sql.append(insert.getStatement().sql).append(" on duplicate key ")
        .append(update.getStatement().sql);
    return sql;
  }

  @Override
  public void setParameter(PreparedStatement stmt) throws DatabaseException {
    update.setParameter(stmt);
    insert.setParameter(stmt, update.getParameterSize() + 1);
  }

}
