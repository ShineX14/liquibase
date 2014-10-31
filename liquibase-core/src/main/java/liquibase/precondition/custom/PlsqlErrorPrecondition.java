package liquibase.precondition.custom;

import java.util.List;
import java.util.Map;

import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.CustomPreconditionErrorException;
import liquibase.exception.CustomPreconditionFailedException;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.precondition.CustomPrecondition;
import liquibase.statement.core.RawSqlStatement;

public class PlsqlErrorPrecondition implements CustomPrecondition {

	private static final Logger logger = LogFactory.getInstance().getLog("Oracle");
	
	private static final String SQL_ERROR = ""//
			+ "select NAME, TYPE, SEQUENCE, LINE, POSITION, TEXT "//
			+ "  from user_errors"//
			+ " where attribute='ERROR'"//
			+ " order by name, type, sequence";

	@Override
	public void check(Database database, DatabaseChangeLog changeLog)
			throws CustomPreconditionFailedException,
			CustomPreconditionErrorException {
		try {
			List<Map<String, ?>> result = ExecutorService.getInstance()
					.getExecutor(database)
					.queryForList(new RawSqlStatement(SQL_ERROR));
			
			String lastObject = "";
			for (Map<String, ?> map : result) {
				String object = map.get("NAME") + "(" + map.get("TYPE") + ")";
				if (!object.equals(lastObject)) {
					logger.severe("----" + object + "----");
					lastObject = object;
				}
				String info = "["+map.get("LINE")+","+map.get("POSITION")+"]"+map.get("TEXT");
				logger.severe(info);
			}

			if (result != null && !result.isEmpty()) {
				throw new CustomPreconditionFailedException(
						"PL/SQL compilation error found.");
			}

		} catch (DatabaseException e) {
			throw new CustomPreconditionErrorException(
					"Unknown database exception", e);
		}
	}

}
