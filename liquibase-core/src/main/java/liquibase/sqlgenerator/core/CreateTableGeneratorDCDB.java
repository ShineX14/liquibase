package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.TencentDCDBDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.CreateTableStatement;


/**
 * An Informix-specific create table statement generator.
 * 
 * @author islavov
 */
public class CreateTableGeneratorDCDB extends CreateTableGenerator {

    @Override
    public boolean supports(CreateTableStatement statement, Database database) {
        return database instanceof TencentDCDBDatabase;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DATABASE;
    }

	@Override
    public Sql[] generateSql(CreateTableStatement statement, Database database, SqlGeneratorChain sqlGeneratorChain) {
        Sql[] sql = super.generateSql(statement, database, sqlGeneratorChain);
        int length = sql.length;
        Sql[] newsql = new Sql[length + 1];
        for (int i=0; i<length; i++) {
          newsql[i] = sql[i];
        }
        if (statement.getShardKey() != null) {
          StringBuilder s = new StringBuilder();
          s.append(sql[0].toSql());
          s.append(" shardkey=").append(statement.getShardKey());
          newsql[0] = new UnparsedSql(s.toString(), getAffectedTable(statement));
        }
        newsql[length] = new UnparsedSql("select sleep(5)", getAffectedTable(statement));
        return newsql;
	}

}
