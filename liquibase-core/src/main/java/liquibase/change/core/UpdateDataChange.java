package liquibase.change.core;

import liquibase.Liquibase;
import liquibase.change.*;
import liquibase.database.Database;
import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.exception.ValidationErrors;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.UpdateStatement;
import liquibase.statement.prepared.UpdateExecutablePreparedStatement;
import liquibase.statement.prepared.UpdateExecutablePreparedStatementChange;
import liquibase.util.StreamUtil;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@DatabaseChange(name = "update", description = "Updates data in an existing table", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "table")
public class UpdateDataChange extends AbstractModifyDataChange implements ChangeWithColumns<ColumnConfig>, UpdateExecutablePreparedStatementChange {

    private List<ColumnConfig> columns;

    public UpdateDataChange() {
        columns = new ArrayList<ColumnConfig>();
    }

    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validate = super.validate(database);
        validate.checkRequiredField("columns", getColumns());
        return validate;
    }

    @Override
    @DatabaseChangeProperty(description = "Data to update", requiredForDatabase = "all")
    public List<ColumnConfig> getColumns() {
        return columns;
    }

    @Override
    public void setColumns(List<ColumnConfig> columns) {
        this.columns = columns;
    }

    @Override
    public void addColumn(ColumnConfig column) {
        columns.add(column);
    }

    public void removeColumn(ColumnConfig column) {
        columns.remove(column);
    }

    private SqlStatement[] sqlStatements;//cache
    
    @Override
    public SqlStatement[] generateStatements(Database database) {

        if (sqlStatements != null) {
            return sqlStatements;
        }

        if (Liquibase.isPreparedStatementPreferred()) {
            sqlStatements = new SqlStatement[] { new UpdateExecutablePreparedStatement(database, this) };
            return sqlStatements;
        }

        UpdateStatement statement = new UpdateStatement(getCatalogName(), getSchemaName(), getTableName());

        for (ColumnConfig column : getColumns()) {
            statement.addNewColumnValue(column.getName(), column.getValueObject());
        }

        statement.setWhereClause(where);

        for (ColumnConfig whereParam : whereParams) {
            if (whereParam.getName() != null) {
                statement.addWhereColumnName(whereParam.getName());
            }
            statement.addWhereParameter(whereParam.getValueObject());
        }

        List<SqlStatement> sqlList = new ArrayList<SqlStatement>();
        sqlList.add(statement);
        
        for (ColumnConfig column : columns) {
          statement.addNewColumnValue(column.getName(), column.getValueObject());
          if (column.getValueClobFile() != null) {
            InputStream stream = StreamUtil.getLobFileStream(getResourceAccessor(), column.getValueBlobFile(), getChangeSet().getFilePath());
            List<String> clobSql = GenerateClobOrBlobSql.generateClobSql(stream, tableName,column.getName(), where);
            sqlList.addAll(GenerateClobOrBlobSql.sqlToRawSqlStatements(clobSql));
          } else if (column.getValueBlobFile() != null) {
            InputStream stream = StreamUtil.getLobFileStream(getResourceAccessor(), column.getValueClobFile(), getChangeSet().getFilePath());
            List<String> blobSql = GenerateClobOrBlobSql.generateBlobSql(stream, tableName,column.getName(), where);
            sqlList.addAll(GenerateClobOrBlobSql.sqlToRawSqlStatements(blobSql));
          }
        }

        sqlStatements = sqlList.toArray(new SqlStatement[sqlList.size()]);
        return sqlStatements;
    }

    @Override
    public ChangeStatus checkStatus(Database database) {
        return new ChangeStatus().unknown("Cannot check updateData status");
    }

    @Override
    public String getConfirmationMessage() {
        return "Data updated in " + getTableName();
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @Override
    protected void customLoadLogic(ParsedNode parsedNode, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        ParsedNode whereParams = parsedNode.getChild(null, "whereParams");
        if (whereParams != null) {
            for (ParsedNode param : whereParams.getChildren(null, "param")) {
                ColumnConfig columnConfig = new ColumnConfig();
                try {
                    columnConfig.load(param, resourceAccessor);
                } catch (ParsedNodeException e) {
                    e.printStackTrace();
                }
                addWhereParam(columnConfig);
            }
        }
    }
    
    public String getPrimaryKey() {
        throw new IllegalStateException();
    }
    
}
