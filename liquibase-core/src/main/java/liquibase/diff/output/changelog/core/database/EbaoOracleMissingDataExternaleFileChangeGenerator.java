package liquibase.diff.output.changelog.core.database;

import liquibase.database.Database;
import liquibase.database.core.OracleDatabase;
import liquibase.diff.output.changelog.core.EbaoMissingDataExternalFileChangeGenerator;
import liquibase.structure.DatabaseObject;

public class EbaoOracleMissingDataExternaleFileChangeGenerator extends EbaoMissingDataExternalFileChangeGenerator {

    private static final String sqlPrefix = "select * from ( select /*+ FIRST_ROWS(n) */ a.*, ROWNUM innerROWNUM from (";
    private static final String sqlSuffix = ") a where ROWNUM <= :MAX ) where innerROWNUM >= :MIN";
	
    @Override
    public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    	if (!(database instanceof OracleDatabase)) {
    		return PRIORITY_NONE;
    	}
    	return super.getPriority(objectType, database);
    }

    public String getPagedSql(String sql, int i) {
        String prefix = sqlPrefix;
        String suffix = sqlSuffix.replace(":MIN", String.valueOf(i * ROWS_PER_FILE + 1))//
                .replace(":MAX", String.valueOf((i + 1) * ROWS_PER_FILE));
        return prefix + sql + suffix;
    }

}
