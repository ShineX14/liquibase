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

    @Override
    public String getPagedSql(String sql, int i, int expectedRows) {
        String prefix = sqlPrefix;
        String suffix = sqlSuffix.replace(":MIN", String.valueOf(i * expectedRows + 1))//
                .replace(":MAX", String.valueOf((i + 1) * expectedRows));
        return prefix + sql + suffix;
    }

}
