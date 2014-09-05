package liquibase.precondition;

import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.CustomPreconditionFailedException;

public class ExampleCustomPrecondition implements CustomPrecondition {

    private String name;
    private String count;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCount() {
        return count;
    }

    public void setCount(String count) {
        this.count = count;
    }

    @Override
    public void check(Database database, DatabaseChangeLog databaseChangeLog) throws CustomPreconditionFailedException {
        System.out.println("Custom precondition ran. Name: "+name+", count: "+count  );

//        throw new CustomPreconditionFailedException("custom precondition failed", new RuntimeException());
    }
}
