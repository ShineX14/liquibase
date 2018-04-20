package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.DB2Database;
import liquibase.database.core.TencentDCDBDatabase;
import liquibase.database.core.DerbyDatabase;
import liquibase.database.core.H2Database;
import liquibase.database.core.HsqlDatabase;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.MSSQLDatabase;
import liquibase.database.core.SybaseASADatabase;
import liquibase.exception.ValidationErrors;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateViewStatement;
import liquibase.structure.core.View;

public class CreateViewGeneratorDCDB extends CreateViewGenerator {

    @Override
    public boolean supports(CreateViewStatement statement, Database database) {
        return database instanceof TencentDCDBDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

    @Override
    public Sql[] generateSql(CreateViewStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
      return new Sql[0];
    }
}
