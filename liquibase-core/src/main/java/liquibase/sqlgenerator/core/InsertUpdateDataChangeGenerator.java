package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.prepared.InsertUpdateExecutablePreparedStatement;

/**
 * Dummy SQL generator for <code>InsertDataChange.ExecutableStatement</code><br>
 */
public class InsertUpdateDataChangeGenerator extends
    AbstractSqlGenerator<InsertUpdateExecutablePreparedStatement> {
  public ValidationErrors validate(
      InsertUpdateExecutablePreparedStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
        return new ValidationErrors();
    }

  public Sql[] generateSql(InsertUpdateExecutablePreparedStatement statement,
      Database database, SqlGeneratorChain sqlGeneratorChain) {
        return new Sql[0];
    }

}
