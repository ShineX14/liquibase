package liquibase.change.core;

import java.util.List;

import liquibase.change.ChangeMetaData;
import liquibase.change.ColumnConfig;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.DeleteStatement;
import liquibase.statement.prepared.DeleteExecutablePreparedStatementChange;

@DatabaseChange(name="delete", description = "Deletes data from an existing table", priority = ChangeMetaData.PRIORITY_DEFAULT, appliesTo = "table")
public class DeleteDataChange extends AbstractModifyDataChange implements DeleteExecutablePreparedStatementChange {


    @Override
    public SqlStatement[] generateStatements(Database database) {

        DeleteStatement statement = new DeleteStatement(getCatalogName(), getSchemaName(), getTableName());

        statement.setWhereClause(where);

        for (ColumnConfig whereParam : whereParams) {
            if (whereParam.getName() != null) {
                statement.addWhereColumnName(whereParam.getName());
            }
            statement.addWhereParameter(whereParam.getValueObject());
        }

        return new SqlStatement[]{
                statement
        };
    }

    @Override
    public String getConfirmationMessage() {
        return "Data deleted from " + getTableName();
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

    @Override
    public void addColumn(ColumnConfig column) {
      addWhereParam(column);
    }

    @Override
    public List<ColumnConfig> getColumns() {
      return getWhereParams();
    }

    @Override
    public void setColumns(List<ColumnConfig> columns) {
      setWhereParam(columns);
    }

    @Override
    public String getPrimaryKey() {
      throw new IllegalStateException();
    }

}
