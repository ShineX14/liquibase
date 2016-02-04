package liquibase.sqlgenerator.core;

import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.statement.core.RawSqlStatement;

public class RawSqlGeneratorOracle extends RawSqlGenerator {

  private static final Logger logger = LogFactory.getLogger();

  @Override
  public int getPriority() {
    return PRIORITY_DATABASE;
  }

  @Override
  public boolean supports(RawSqlStatement statement, Database database) {
    return database instanceof OracleDatabase;
  }

  private static final String[] PROMPT_KEYS = new String[] {"prompt ", "PROMPT ", "set define off", "set feedback off"};
  private static final String[] EXEC_KEYS = new String[] {"exec ", "EXEC ", "execute ", "EXECUTE "};
  
  @Override
  public Sql[] generateSql(RawSqlStatement statement, Database database,
      SqlGeneratorChain sqlGeneratorChain) {
    String sql = statement.getSql();
    while (isPromptStatement(sql)) {
      int index = sql.indexOf("\n");
      if (index < 0) {
        logger.info(sql);
        return null;
      }
      logger.info(sql.substring(0, index));
      sql = sql.substring(index + 1);
    }
    
    for (String key : EXEC_KEYS) {
      if (sql.startsWith(key)) {
        sql = "call " + sql.substring(key.length());
        if (!sql.endsWith(")")) {
          sql = sql + "()";
        }
        logger.debug(sql);
        break;
      }
	}

    return new Sql[] { new UnparsedSql(sql, statement.getEndDelimiter()) };
  }

  private boolean isPromptStatement(String sql) {
    for (String key : PROMPT_KEYS) {
	  if (sql.startsWith(key)) {
		return true;
      }
	}
    return false;
  }

}
