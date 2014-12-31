package liquibase.precondition.custom;

import java.math.BigDecimal;
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
import liquibase.util.StringUtils;

public class PlsqlErrorPrecondition implements CustomPrecondition {

	private static final Logger logger = LogFactory.getInstance().getLog("Oracle");
	
	private static final String SQL_ERROR = ""//
			+ "select e.NAME, e.TYPE, e.SEQUENCE, e.LINE, e.POSITION, e.TEXT, s.TEXT SOURCE_TEXT "//
			+ "  from user_errors e, user_source s"//
			+ " where e.name = s.name"//
			+ "   and e.type = s.type"//
			+ "   and e.line = s.line"//
			+ "   and e.attribute = 'ERROR'"//
			+ "   and e.name not like 'BIN$%'"//
			+ " order by e.name, e.type, e.sequence";

	@Override
	public void check(Database database, DatabaseChangeLog changeLog)
			throws CustomPreconditionFailedException,
			CustomPreconditionErrorException {
		try {
			List<Map<String, ?>> result = ExecutorService.getInstance()
					.getExecutor(database)
					.queryForList(new RawSqlStatement(SQL_ERROR));
			
			String lastObject = "";
			BigDecimal lastLine = BigDecimal.ZERO;
			for (Map<String, ?> map : result) {
				String object = map.get("NAME") + "(" + map.get("TYPE") + ")";
				if (!object.equals(lastObject)) {
					logger.severe("----" + StringUtils.repeat("-", object.length()) + "----");
					logger.severe("----" + object + "----");
					lastObject = object;
					lastLine = BigDecimal.ZERO;
				}
				BigDecimal line = (BigDecimal) map.get("LINE");
				if (!lastLine.equals(line)) {
					logger.severe(trim((String)map.get("SOURCE_TEXT")));
					logger.severe(StringUtils.repeat(" ", ((BigDecimal)map.get("POSITION")).intValue() - 1) + "^");
					lastLine = line;
				}
				logger.severe("[" + line + "," + map.get("POSITION") + "]" + map.get("TEXT"));
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

	private String trim(String sourceText) {
		if (sourceText.endsWith("\r\n")) {
			return sourceText.substring(0, sourceText.length() - 2);
		} else if (sourceText.endsWith("\r") || sourceText.endsWith("\n")) {
			return sourceText.substring(0, sourceText.length() - 1);
		} else {
			return sourceText;
		}
	}

}
