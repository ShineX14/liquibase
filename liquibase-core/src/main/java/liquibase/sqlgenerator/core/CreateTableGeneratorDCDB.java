package liquibase.sqlgenerator.core;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import liquibase.database.Database;
import liquibase.database.core.InformixDatabase;
import liquibase.database.core.TencentDCDBDatabase;
import liquibase.exception.ValidationErrors;
import liquibase.logging.LogFactory;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;
import liquibase.statement.AutoIncrementConstraint;
import liquibase.statement.ForeignKeyConstraint;
import liquibase.statement.UniqueConstraint;
import liquibase.statement.core.CreateTableStatement;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import liquibase.util.StringUtils;


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
        if (statement.getShardKey() != null) {
          StringBuilder s = new StringBuilder();
          s.append(sql[0].toSql());
          s.append(" shardkey=").append(statement.getShardKey());
          sql[0] = new UnparsedSql(s.toString(), getAffectedTable(statement));
        }
        return sql;
	}

}
