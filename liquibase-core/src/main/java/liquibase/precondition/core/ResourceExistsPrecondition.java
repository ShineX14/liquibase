package liquibase.precondition.core;

import java.net.URL;

import liquibase.changelog.DatabaseChangeLog;
import liquibase.database.Database;
import liquibase.exception.CustomPreconditionErrorException;
import liquibase.exception.CustomPreconditionFailedException;
import liquibase.precondition.CustomPrecondition;

public class ResourceExistsPrecondition implements CustomPrecondition {

  private String resourceName;

  private final boolean relativeToChangelogFile = true;

  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public boolean isRelativeToChangelogFile() {
    return relativeToChangelogFile;
  }

  public void check(Database database, DatabaseChangeLog changeLog)
      throws CustomPreconditionFailedException,
      CustomPreconditionErrorException {
    try {
      URL resource = getClass().getResource(resourceName);
      if (resource == null) {
        throw new CustomPreconditionFailedException("Resource " + getResourceName() + " does not exist");
      }
    } catch (CustomPreconditionFailedException e) {
      throw e;
    } catch (Exception e) {
      throw new CustomPreconditionErrorException("Error in loading resource " + getResourceName() , e);
    }
  }

}
