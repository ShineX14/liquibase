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

public class PostgresUpsertExecutableStatement extends
		AbstractPreparedStatement {

	private final Database database;
	private final InsertExecutablePreparedStatementChange change;

	private Info statement;
	private final List<ColumnConfig> cols = new ArrayList<ColumnConfig>();

	public PostgresUpsertExecutableStatement(Database database,
			InsertExecutablePreparedStatementChange change) {
		this.database = database;
		this.change = change;
	}

	@Override
	public Info getStatement() {
		if (statement != null) {
			return statement;
		}

		StringBuilder params = new StringBuilder();
		StringBuilder fields = new StringBuilder();
		StringBuilder fieldsSet = new StringBuilder();
		StringBuilder where1 = new StringBuilder();
		StringBuilder where2 = new StringBuilder();

		String tableName = database.escapeTableName(change.getCatalogName(),
				change.getSchemaName(), change.getTableName());

		for (ColumnConfig column : change.getColumns()) {
			if (database.supportsAutoIncrement()
					&& Boolean.TRUE.equals(column.isAutoIncrement())) {
				continue;
			}
			String columnName = database.escapeColumnName(
					change.getCatalogName(), change.getSchemaName(),
					change.getTableName(), column.getName());

			fields.append(columnName).append(", ");
			fieldsSet.append(columnName).append("=nv.").append(columnName)
					.append(", ");

			if (column.getValueObject() == null
					&& column.getValueBlobFile() == null
					&& column.getValueClobFile() == null) {
				params.append("null, ");
			} else {
				if (column.getValueDate() != null) {
					params.append("date(?), ");
				} else {
					params.append("?, ");
				}
				cols.add(column);
			}
		}

		params.deleteCharAt(params.lastIndexOf(" "));
		params.deleteCharAt(params.lastIndexOf(","));
		fields.deleteCharAt(fields.lastIndexOf(" "));
		fields.deleteCharAt(fields.lastIndexOf(","));
		fieldsSet.deleteCharAt(fieldsSet.lastIndexOf(" "));
		fieldsSet.deleteCharAt(fieldsSet.lastIndexOf(","));

		StringBuilder sql = new StringBuilder();
		sql.append("with new_values( ").append(fields).append(" ) as (");
		sql.append("values( ").append(params).append(")), ");
		sql.append("upsert as ( update ").append(tableName).append(" m ");
		sql.append("set ").append(fieldsSet)
				.append(" from new_values nv where ").append(where1);
		sql.append(" returning m.*) ");
		sql.append("insert into ").append(tableName).append("(").append(fields)
				.append(") ");
		sql.append("select ").append(fields).append(" from new_values ");
		sql.append("where not exists(select 1 from upsert up where ")
				.append(where2).append(") ");

		String s = sql.toString();
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
