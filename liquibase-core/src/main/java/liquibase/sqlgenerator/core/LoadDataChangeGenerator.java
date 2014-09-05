package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.prepared.LoadExecutablePreparedStatement;

/**
 * Dummy SQL generator for <code>InsertDataChange.ExecutableStatement</code><br>
 */
public class LoadDataChangeGenerator extends
    AbstractSqlGenerator<LoadExecutablePreparedStatement> {
  public ValidationErrors validate(LoadExecutablePreparedStatement statement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

  public Sql[] generateSql(LoadExecutablePreparedStatement statement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }

}
