package liquibase.changelog;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.parser.core.ParsedNode;
import liquibase.parser.core.ParsedNodeException;
import liquibase.resource.ResourceAccessor;

public class IncludedFile extends ChangeSet {

    private final String fileName;
    private final String tableName;

    public IncludedFile(String fileName, String tableName) {
        super(null, null, false, false, null, null, null, null);
        this.fileName = fileName;
        this.tableName = tableName;
    }

    public String getFileName() {
        return fileName;
    }

    
    public String getTableName() {
        return tableName;
    }

    @Override
    public int hashCode() {
        return fileName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof IncludedFile)) {
            return false;
        }

        IncludedFile other = (IncludedFile) obj;
        return fileName.equals(other.fileName);
    }

    @Override
    public String getSerializedObjectName() {
        return "include";
    }

    @Override
    public Set<String> getSerializableFields() {
        return new HashSet<String>(Arrays.asList("file"));

    }

    @Override
    public Object getSerializableFieldValue(String field) {
        if (field.equals("file")) {
            return this.getFileName();
        }

        throw new UnexpectedLiquibaseException("Unexpected field request on changeSet: " + field);
    }

    @Override
    public SerializationType getSerializableFieldType(String field) {
        return SerializationType.NAMED_FIELD;
    }

    @Override
    public String getSerializedObjectNamespace() {
        return STANDARD_CHANGELOG_NAMESPACE;
    }

    @Override
    public ParsedNode serialize() {
        throw new RuntimeException("TODO");
    }

    @Override
    public void load(ParsedNode node, ResourceAccessor resourceAccessor) throws ParsedNodeException {
        throw new UnsupportedOperationException();//DatabaseChangeLog.loadIncludedDatabaseChangeLog
    }

}
