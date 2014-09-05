package liquibase.statement.prepared;

import java.sql.PreparedStatement;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.database.core.OracleDatabase;
import liquibase.database.core.PostgresDatabase;
import liquibase.exception.DatabaseException;
import liquibase.statement.ExecutablePreparedStatement;

public class InsertUpdateExecutablePreparedStatement extends
    AbstractPreparedStatement implements ExecutablePreparedStatement {

  private final Database database;
  private final InsertExecutablePreparedStatement insert;
  private final InsertExecutablePreparedStatement update;
  private final InsertExecutablePreparedStatementChange change;
  private Info statement;

  public InsertUpdateExecutablePreparedStatement(Database database,
      InsertExecutablePreparedStatementChange change) {
    this.database = database;
    this.insert = new InsertExecutablePreparedStatement(database, change);

    if (database instanceof MySQLDatabase) {
      this.update = new InsertExecutablePreparedStatement(database, change, 3);
    } else if(database instanceof PostgresDatabase){
    	this.update = new InsertExecutablePreparedStatement(database, change, 4);
    } else{
    	this.update = new InsertExecutablePreparedStatement(database, change, 2);
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
    } else if(database instanceof PostgresDatabase){
      sql = getPostgresSQL();
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
  
  private StringBuilder getPostgresSQL() {
	    StringBuilder sql = new StringBuilder();
//	    sql.append("DO LANGUAGE plpgsql $$\n");
//	    sql.append("BEGIN\n  ");
//	    sql.append(update.getStatement().sql).append(";\n  ");
//	    sql.append("IF NOT FOUND THEN\n    ");
//	    sql.append(insert.getStatement().sql).append(";\n  ");
//	    sql.append("END IF;\n");
//	    sql.append("END $$; {escape '$'}\n");
	    sql.append(update.getStatement().sql).append(";\n");
//	    sql.append(insert.getStatement().sql).append(";\n");
	    return sql;
	  }

  private StringBuilder getMysqlSql() {
    StringBuilder sql = new StringBuilder();
    sql.append(insert.getStatement().sql).append(" on duplicate key ")
        .append(update.getStatement().sql);
    return sql;
  }

  @Override
  protected void setParameter(PreparedStatement stmt) throws DatabaseException {
    update.setParameter(stmt);
    if(!(database instanceof PostgresDatabase)){
    insert.setParameter(stmt, update.getParameterSize() + 1);
    }
  }

}
