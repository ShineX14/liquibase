package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.TencentDCDBDatabase;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.AddForeignKeyConstraintStatement;
import liquibase.statement.core.AddUniqueConstraintStatement;

public class AddForeignKeyConstraintGeneratorDCDB extends AddForeignKeyConstraintGenerator {

    @Override
    public boolean supports(AddForeignKeyConstraintStatement statement, Database database) {
        return database instanceof TencentDCDBDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(AddForeignKeyConstraintStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
      return new Sql[0];
    }
}
