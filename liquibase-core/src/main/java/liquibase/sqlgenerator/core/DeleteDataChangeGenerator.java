package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.prepared.DeleteExecutablePreparedStatement;

/**
 * Dummy SQL generator for <code>InsertDataChange.ExecutableStatement</code><br>
 */
public class DeleteDataChangeGenerator extends
    AbstractSqlGenerator<DeleteExecutablePreparedStatement> {
  @Override
  public ValidationErrors validate(
      DeleteExecutablePreparedStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

  @Override
  public Sql[] generateSql(DeleteExecutablePreparedStatement statement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }

}
