package liquibase.diff.output.changelog.core.database;

import liquibase.database.Database;
import liquibase.database.core.MySQLDatabase;
import liquibase.diff.output.changelog.core.EbaoMissingDataExternalFileChangeGenerator;
import liquibase.structure.DatabaseObject;

public class EbaoMysqlMissingDataExternaleFileChangeGenerator extends EbaoMissingDataExternalFileChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    	if (!(database instanceof MySQLDatabase)) {
    		return PRIORITY_NONE;
    	}
    	return super.getPriority(objectType, database);
    }

    @Override
    public String getPagedSql(String sql, int i, int expectedRows) {
      return sql + " limit " + i * expectedRows + "," + expectedRows;
    }

}
