package liquibase.integration.ant;

import org.apache.tools.ant.BuildException;

import liquibase.Liquibase;

public class ReleaseLocksTask extends BaseLiquibaseTask {

	@Override
	protected void executeWithLiquibaseClassloader() throws BuildException {
        if (!shouldRun()) {
            return;
        }

        Liquibase liquibase = null;
        try {
            liquibase = createLiquibase();
            liquibase.forceReleaseLocks();
        } catch (Exception e) {
            throw new BuildException(e);
        } finally {
            closeDatabase(liquibase);
        }
        
	}

}
