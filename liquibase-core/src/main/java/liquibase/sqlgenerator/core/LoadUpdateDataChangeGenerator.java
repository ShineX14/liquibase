package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.prepared.LoadUpdateExecutablePreparedStatement;

/**
 * Dummy SQL generator for <code>InsertDataChange.ExecutableStatement</code><br>
 */
public class LoadUpdateDataChangeGenerator extends
    AbstractSqlGenerator<LoadUpdateExecutablePreparedStatement> {
  public ValidationErrors validate(
      LoadUpdateExecutablePreparedStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

  public Sql[] generateSql(LoadUpdateExecutablePreparedStatement statement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }

}
