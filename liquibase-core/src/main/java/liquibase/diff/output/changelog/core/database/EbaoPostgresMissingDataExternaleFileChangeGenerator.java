package liquibase.diff.output.changelog.core.database;

import liquibase.database.Database;
import liquibase.database.core.PostgresDatabase;
import liquibase.diff.output.changelog.core.EbaoMissingDataExternalFileChangeGenerator;
import liquibase.structure.DatabaseObject;

public class EbaoPostgresMissingDataExternaleFileChangeGenerator extends EbaoMissingDataExternalFileChangeGenerator {

    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    	if (!(database instanceof PostgresDatabase)) {
    		return PRIORITY_NONE;
    	}
    	return super.getPriority(objectType, database);
    }

    public String getPagedSql(String sql, int i) {
      return sql + " limit " + ROWS_PER_FILE + " offset " + (i * ROWS_PER_FILE);
    }

}
