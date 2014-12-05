package liquibase.statement.prepared.database;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import liquibase.change.ColumnConfig;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.statement.prepared.AbstractPreparedStatement;
import liquibase.statement.prepared.InsertExecutablePreparedStatementChange;

public class MssqlUpsertExecutableStatement extends AbstractPreparedStatement {

	private final Database database;
	private final InsertExecutablePreparedStatementChange change;

	private Info statement;
	private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

	public MssqlUpsertExecutableStatement(Database database,
			InsertExecutablePreparedStatementChange change) {
		this.database = database;
		this.change = change;
	}

	public Info getStatement() {
		if (statement != null) {
			return statement;
		}

		String tableName = database.escapeTableName(change.getCatalogName(),
				change.getSchemaName(), change.getTableName());

		StringBuilder columnSql = new StringBuilder();
		StringBuilder columnValueSql = new StringBuilder();
		StringBuilder insertColumnSql = new StringBuilder();
		StringBuilder insertValueSql = new StringBuilder();
		StringBuilder updateSql = new StringBuilder();

		List<String> primaryKeys = getPrimaryKey(change.getPrimaryKey());
		for (ColumnConfig column : change.getColumns()) {
			if (database.supportsAutoIncrement()
					&& Boolean.TRUE.equals(column.isAutoIncrement())) {
				continue;
			}

			String columnName = database.escapeColumnName(
					change.getCatalogName(), change.getSchemaName(),
					change.getTableName(), column.getName());

			insertColumnSql.append(columnName).append(",");

			boolean pkcloum = primaryKeys.contains(columnName);
			if (column.getValueObject() == null
					&& column.getValueBlobFile() == null
					&& column.getValueClobFile() == null) {
				insertValueSql.append("null,");
				if (!pkcloum) {
					updateSql.append(columnName + "=null,");
				}
			} else if (column.getValueComputed() != null) {
				String value = column.getValueComputed().getValue();
				insertValueSql.append(value + ",");
				if (!pkcloum) {
					updateSql.append(columnName + "=" + value + ",");
				}
			} else {
				columnSql.append(columnName + ",");
				columnValueSql.append("?,");
				cols.add(column);

				insertValueSql.append("s." + columnName + ",");
				if (!pkcloum) {
					updateSql.append(columnName + "=s." + columnName + ",");
				}
			}
		}

		columnSql.deleteCharAt(columnSql.lastIndexOf(","));
		columnValueSql.deleteCharAt(columnValueSql.lastIndexOf(","));
		insertColumnSql.deleteCharAt(insertColumnSql.lastIndexOf(","));
		insertValueSql.deleteCharAt(insertValueSql.lastIndexOf(","));
		updateSql.deleteCharAt(updateSql.lastIndexOf(","));

		StringBuilder mergeSql = new StringBuilder();
		mergeSql.append("merge" + tableName + " t ");
		mergeSql.append("using (values(" + columnValueSql + ")) as s("
				+ columnSql + ")");
		String onClause = getPrimaryKeyClause(change.getPrimaryKey(), "s", "t");
		mergeSql.append(" on " + onClause);
		mergeSql.append(" when matched then update set " + updateSql);
		mergeSql.append(" when not matched then");
		mergeSql.append(" insert(" + insertColumnSql + ")");
		mergeSql.append(" values(" + insertValueSql + ");");

		String s = mergeSql.toString();
		statement = new Info(s, cols, getParameters(cols, change.getChangeSet()
				.getFilePath()));
		return statement;
	}

	@Override
	public void setParameter(PreparedStatement stmt) throws DatabaseException {
		try {
			setParameter(stmt, 1, cols, change.getChangeSet().getChangeLog()
					.getPhysicalFilePath(), change.getResourceAccessor());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}
}
