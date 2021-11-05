package liquibase.diff.output.changelog.core;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Hex;

import liquibase.change.Change;
import liquibase.change.ColumnConfig;
import liquibase.change.core.AddForeignKeyConstraintChange;
import liquibase.change.core.AddPrimaryKeyChange;
import liquibase.change.core.AddUniqueConstraintChange;
import liquibase.change.core.CreateIndexChange;
import liquibase.change.core.CreateSequenceChange;
import liquibase.change.core.CreateTableChange;
import liquibase.change.core.CreateViewChange;
import liquibase.change.core.InsertDataChange;
import liquibase.change.core.InsertUpdateDataChange;
import liquibase.change.core.LoadDataChange;
import liquibase.change.core.LoadDataColumnConfig;
import liquibase.change.core.LoadUpdateDataChange;
import liquibase.changelog.ChangeSet;
import liquibase.changelog.IncludedFile;
import liquibase.database.Database;
import liquibase.database.jvm.JdbcConnection;
import liquibase.diff.output.ChangeSetToChangeLog;
import liquibase.diff.output.DataInterceptor;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.diff.output.changelog.ChangeGeneratorChain;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Column;
import liquibase.structure.core.Data;
import liquibase.structure.core.PrimaryKey;
import liquibase.structure.core.Table;
import liquibase.util.ISODateFormat;
import liquibase.util.csv.CSVWriter;

public abstract class EbaoMissingDataExternalFileChangeGenerator extends MissingDataExternalFileChangeGenerator {

  private final Logger logger = LogFactory.getInstance().getLog();

  public EbaoMissingDataExternalFileChangeGenerator() {
  }

  @Override
  public int getPriority(Class<? extends DatabaseObject> objectType, Database database) {
    int priority = super.getPriority(objectType, database);
    if (PRIORITY_NONE != priority) {
      priority = priority + 1;
    }
    return priority;
  }

  @Override
  public ChangeSet[] fixMissing(DatabaseObject missingObject, DiffOutputControl outputControl, Database referenceDatabase,
      Database comparisionDatabase, ChangeGeneratorChain chain) {
    Data data = (Data) missingObject;

    Table table = data.getTable();
    if (referenceDatabase.isLiquibaseObject(table)) {
      return null;
    }

    EbaoDiffOutputControl ebaoOutputControl = (EbaoDiffOutputControl) outputControl;

    List<IncludedFile> changes = new ArrayList<IncludedFile>();
    try {
      for (EbaoDiffOutputControl.TableCondition filter : ebaoOutputControl.getDiffWhereClause(table.getName())) {
        List<IncludedFile> change = fixMissing(ebaoOutputControl, referenceDatabase, table, filter);
        changes.addAll(change);
      }
    } catch (Exception e) {
      throw new UnexpectedLiquibaseException(e);
    }

    return changes.toArray(new ChangeSet[changes.size()]);
  }

  protected abstract String getPagedSql(String sql, int i, int expectedRows);

  private List<IncludedFile> fixMissing(EbaoDiffOutputControl outputControl, Database referenceDatabase, Table table,
      EbaoDiffOutputControl.TableCondition filter) throws DatabaseException, IOException, ParserConfigurationException {
    String escapedTableName = referenceDatabase.escapeTableName(table.getSchema().getCatalogName(), table.getSchema().getName(),
        table.getName());
    String sql = "SELECT * FROM " + escapedTableName;
    String sqlRowCount = "SELECT count(1) FROM " + escapedTableName;

    String condition = filter.getCondition();
    String lowerCaseCondition = condition;
    if (condition != null) {
      lowerCaseCondition = condition.toLowerCase();
    }
    if (condition != null && !"".equals(condition)) {
      sql = sql + " " + condition;
      int i = lowerCaseCondition.indexOf("order by");
      sqlRowCount = sqlRowCount + " " + (i < 0 ? condition : condition.substring(0, i));
    }
    if (condition != null && !lowerCaseCondition.contains("order by") && !lowerCaseCondition.contains("connect by")) {
      PrimaryKey primaryKey = table.getPrimaryKey();
      if (primaryKey != null) {
        for (int i = 0; i < primaryKey.getColumnNamesAsList().size(); i++) {
          sql = sql + (i == 0 ? " order by " : ",");
          sql = sql + primaryKey.getColumnNamesAsList().get(i);
        }
      }
    }

    List<IncludedFile> changeSets = new ArrayList<IncludedFile>();

    JdbcConnection connection = (JdbcConnection) referenceDatabase.getConnection();

    int rowCount = executeQueryRowCount(connection, sqlRowCount);
    if (rowCount == 0) {
      logger.info("no data found in " + escapedTableName);
      return changeSets;
    }

    logger.info("loading data of " + table + "(" + rowCount + ")");
    if (rowCount <= outputControl.getXmlCsvRowLimit()) {
      List<Map<String, Object>> rs = executeQuery(connection, sql, outputControl.getCsvRowLimit());
      String filename = filter.getFilename() != null ? filter.getFilename() : table.getName();
      IncludedFile includedFile = addInsertDataChanges(outputControl, table, rs, outputControl.getDataDir(), filter.getSubdir(), filename, false);
      if (includedFile != null) {
        changeSets.add(includedFile);
      }
    } else {
      for (int i = 0; i <= (rowCount - 1) / outputControl.getCsvRowLimit(); i++) {
        String sqlRowBlock = getPagedSql(sql, i, outputControl.getCsvRowLimit());
        List<Map<String, Object>> rs = executeQuery(connection, sqlRowBlock, outputControl.getCsvRowLimit());

        String filename = filter.getFilename() != null ? filter.getFilename() : table.getName();
        if ((rowCount - 1) / outputControl.getCsvRowLimit() > 0) {
          filename = filename + "." + (i + 1);
        }

        IncludedFile includedFile = addInsertDataChanges(outputControl, table, rs, outputControl.getDataDir(), filter.getSubdir(), filename, true);
        if (includedFile != null) {
          changeSets.add(includedFile);
        }
      }
    }

    return changeSets;
  }

  private int executeQueryRowCount(JdbcConnection connection, String sql) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement();
      rs = stmt.executeQuery(sql);
      if (rs.next()) {
        return rs.getInt(1);
      }
      throw new IllegalStateException(sql);
    } catch (Exception e) {
      throw new UnexpectedLiquibaseException(sql, e);
    } finally {
      close(stmt, rs);
    }
  }

  private List<Map<String, Object>> executeQuery(JdbcConnection connection, String sql, int expectedRows) {
    Statement stmt = null;
    ResultSet rs = null;
    try {
      stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

      rs = stmt.executeQuery(sql);

      List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(expectedRows);
      ResultSetMetaData md = rs.getMetaData();
      int columns = md.getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new HashMap<String, Object>();
        for (int i = 1; i <= columns; ++i) {
          Object value = rs.getObject(i);
          if (value instanceof Blob || value instanceof byte[]) {
            value = rs.getBytes(i);
          } else if (value instanceof Clob) {
            value = rs.getString(i);
          }
          row.put(md.getColumnName(i), value);
        }
        list.add(row);
      }

      if (list.isEmpty()) {
        throw new IllegalStateException("Empty result set: " + sql);
      }
      return list;
    } catch (Exception e) {
      throw new UnexpectedLiquibaseException(sql, e);
    } finally {
      close(stmt, rs);
    }
  }

  private void close(Statement stmt, ResultSet rs) {
    if (rs != null) {
      try {
        rs.close();
      } catch (SQLException ignore) {
      }
    }
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignore) {
      }
    }
  }

  private static final String fileextension1 = ".data.xml";
  private static final String fileextension2 = ".xml";

  private IncludedFile addInsertDataChanges(DiffOutputControl outputControl, Table table, List<Map<String, Object>> rs, String rootDir,
      String subDir, String fileName, boolean csv) throws DatabaseException, IOException, ParserConfigurationException {
    String dataDir = rootDir;
    if (subDir != null) {
      dataDir = rootDir + "/" + subDir;
    }
    File dir = new File(dataDir);
    if (!dir.exists()) {
      boolean done = dir.mkdirs();
      if (!done) {
        throw new IllegalStateException(dataDir);
      }
    }

    List<String> columnNames = new ArrayList<String>();
    for (Column column : table.getColumns()) {
      columnNames.add(column.getName());
    }
    for (String c : rs.get(0).keySet()) {
      if (!columnNames.contains(c) && !"innerROWNUM".equalsIgnoreCase(c)) {//EbaoOracleMissingDataExternaleFileChangeGenerator
        throw new IllegalStateException("Run 'mvn clean' to refresh database column cache for " + table.getName() + "." + c
            + " is not found in cached columns " + columnNames);
      }
    }
    updateUserId(table.getName(), columnNames, rs);

    EbaoDiffOutputControl ebaoDiffOutputControl = (EbaoDiffOutputControl)outputControl;
    String id = table.getName() + ".DATA";
    ChangeSet changeSet = ChangeSetUtils.generateChangeSet(id, ebaoDiffOutputControl.getChangeSetAuthor(), ebaoDiffOutputControl.isInsertUpdatePreferred());
    if (csv) {
      LoadDataChange change = addInsertDataChangesCsv(ebaoDiffOutputControl, table, columnNames, rs, dataDir, fileName + ".csv");
      changeSet.addChange(change);
    } else {
      List<InsertDataChange> list = addInsertDataChangesXml(ebaoDiffOutputControl, table, columnNames, rs, dataDir);
      for (InsertDataChange change : list) {
        changeSet.addChange(change);
      }
    }

    ChangeSetToChangeLog changeLogWriter = new ChangeSetToChangeLog(changeSet);
    String fileextension = fileName.contains(".") ? fileextension2 : fileextension1;
    String filepath = fileName + fileextension;
    if (dataDir != null) {
      filepath = dataDir + "/" + filepath;
    }
    changeLogWriter.print(filepath);

    String relativeFilePath = fileName + fileextension;
    if (subDir != null) {
      relativeFilePath = subDir + "/" + relativeFilePath;
    }
    IncludedFile includedFile = new IncludedFile(relativeFilePath, table.getName());
    return includedFile;
  }

  private List<InsertDataChange> addInsertDataChangesXml(EbaoDiffOutputControl outputControl, Table table, List<String> columnNames,
      List<Map<String, Object>> rs, String dataDir) throws FileNotFoundException, IOException, DatabaseException,
  ParserConfigurationException {
    List<InsertDataChange> changes = new ArrayList<InsertDataChange>();
    for (Map<String, Object> row : rs) {
      InsertDataChange change = newInsertDataChange(table, outputControl.isInsertUpdatePreferred());
      if (outputControl.getIncludeCatalog()) {
        change.setCatalogName(table.getSchema().getCatalogName());
      }
      if (outputControl.getIncludeSchema()) {
        change.setSchemaName(table.getSchema().getName());
      }
      change.setTableName(table.getName());

      // loop over all columns for this row
      for (String columnName : columnNames) {
        ColumnConfig column = new ColumnConfig();
        column.setName(columnName);

        Object value = row.get(columnName);
        if (value == null) {
          value = row.get(columnName.toUpperCase());
        }
        if (value == null) {
          column.setValue(null);
        } else if (value instanceof Number) {
          column.setValueNumeric((Number) value);
        } else if (value instanceof Boolean) {
          column.setValueBoolean((Boolean) value);
        } else if (value instanceof Date) {
          column.setValueDate((Date) value);
        } else if (table.getColumn(column.getName()).getType().getTypeName().contains("BLOB")) {
          String lobFileName = writeLobFile(table, table.getColumn(column.getName()), value, row, dataDir);
          column.setValueBlobFile(lobFileName);
        } else if (table.getColumn(column.getName()).getType().getTypeName().contains("CLOB")
            || table.getColumn(column.getName()).getType().getTypeName().contains("TEXT")) {
          String lobFileName = writeLobFile(table, table.getColumn(column.getName()), value, row, dataDir);
          column.setValueClobFile(lobFileName);
        } else if (value instanceof byte[]) {
          column.setValueBlob(Hex.encodeHexString((byte[]) value));
        } else { // string
          // column.setValue(value.toString().replace("\\", "\\\\"));
          column.setValue(value.toString());
        }

        change.addColumn(column);
      }

      // for each row, add a new change
      // (there will be one group per table)
      changes.add(change);
    }

    return changes;
  }

  private String writeLobFile(Table table, Column column, Object value, Map<String, Object> row, String dataDir) throws IOException {
    String filename = table.getName() + "." + column.getName();
    PrimaryKey primaryKey = table.getPrimaryKey();
    for (String pkColumnName : primaryKey.getColumnNamesAsList()) {
      filename = filename + "." + row.get(pkColumnName);
    }
    filename = filename + ".lob";

    File dir = new File("lob");
    File f = new File(filename);
    if (dataDir != null) {
      dir = new File(dataDir, "lob");
      f = new File(dir, filename);
    }
    if (!dir.exists()) {
      dir.mkdir();
    }

    if (column.getType().getTypeName().contains("BLOB")) {
      FileOutputStream out = new FileOutputStream(f);
      out.write((byte[]) value);
      out.close();
    } else {
      Writer out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), Charset.forName("UTF-8")));
      out.append((String) value);
      out.close();
    }
    return "lob/" + filename;
  }

  private LoadDataChange addInsertDataChangesCsv(EbaoDiffOutputControl outputControl, Table table, List<String> columnNames,
      List<Map<String, Object>> rs, String dataDir, String fileName) throws IOException {
    // String fileName = table.getName().toLowerCase() + ".csv";
    String filePath = fileName;
    if (dataDir != null) {
      filePath = dataDir + "/" + fileName;
    }

    CSVWriter outputFile = new CSVWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filePath), Charset.forName("UTF-8"))));
    String[] dataTypes = new String[columnNames.size()];
    String[] line = new String[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      line[i] = columnNames.get(i);
    }
    outputFile.writeNext(line);

    for (Map<String, Object> row : rs) {
      line = new String[columnNames.size()];

      for (int i = 0; i < columnNames.size(); i++) {
        Object value = row.get(columnNames.get(i));
        if (value == null) {
          value = row.get(columnNames.get(i).toUpperCase());
        }
        if (dataTypes[i] == null && value != null) {
          if (value instanceof Number) {
            dataTypes[i] = "NUMERIC";
          } else if (value instanceof Boolean) {
            dataTypes[i] = "BOOLEAN";
          } else if (value instanceof Date) {
            dataTypes[i] = "DATE";
          } else if (table.getColumn(columnNames.get(i)).getType().getTypeName().contains("BLOB")) {
            dataTypes[i] = "BLOB";
          } else if (table.getColumn(columnNames.get(i)).getType().getTypeName().contains("CLOB")
              || table.getColumn(columnNames.get(i)).getType().getTypeName().contains("TEXT")) {
            dataTypes[i] = "CLOB";
          } else if (value instanceof byte[]) {
            dataTypes[i] = "BLOB";
          } else {
            dataTypes[i] = "STRING";
          }
        }
        if (value == null) {
          // line[i] = "NULL";
          line[i] = null;
        } else {
          if (value instanceof Date) {
            line[i] = new ISODateFormat().format((Date) value);
          } else if (table.getColumn(columnNames.get(i)).getType().getTypeName().contains("BLOB")) {
            String lobFileName = writeLobFile(table, table.getColumn(columnNames.get(i)), value, row, dataDir);
            line[i] = lobFileName;
          } else if (table.getColumn(columnNames.get(i)).getType().getTypeName().contains("CLOB")
              || table.getColumn(columnNames.get(i)).getType().getTypeName().contains("TEXT")) {
            String lobFileName = writeLobFile(table, table.getColumn(columnNames.get(i)), value, row, dataDir);
            line[i] = lobFileName;
          } else if (value instanceof byte[]) {
            line[i] = Hex.encodeHexString((byte[]) value);
          } else {
            line[i] = value.toString();
          }
        }
      }
      outputFile.writeNext(line);
    }
    outputFile.flush();
    outputFile.close();

    LoadDataChange change = newLoadDataChange(table, outputControl.isInsertUpdatePreferred());
    change.setFile(fileName);
    change.setEncoding("UTF-8");
    if (outputControl.getIncludeCatalog()) {
      change.setCatalogName(table.getSchema().getCatalogName());
    }
    if (outputControl.getIncludeSchema()) {
      change.setSchemaName(table.getSchema().getName());
    }
    change.setTableName(table.getName());

    for (int i = 0; i < columnNames.size(); i++) {
      String colName = columnNames.get(i);
      LoadDataColumnConfig columnConfig = new LoadDataColumnConfig();
      columnConfig.setHeader(colName);
      columnConfig.setName(colName);
      columnConfig.setType(dataTypes[i]);

      change.addColumn(columnConfig);
    }
    return change;
  }

  private void updateUserId(String tableName, List<String> columnNames, List<Map<String, Object>> rs) {
    for (String column : columnNames) {
      String key = column.toUpperCase();
      Long userIdValue = DataInterceptor.getUserIdColumnValue(tableName, column);
      if (userIdValue != null) {
        for (Map<String, Object> row : rs) {
          Object value = row.get(key);
          if (value != null && value instanceof Number) {
            row.put(key, userIdValue);
          }
        }
      }
    }
  }

  private InsertDataChange newInsertDataChange(Table table, boolean insertUpdatePreferred) {
    if (insertUpdatePreferred) {
      InsertUpdateDataChange change = new InsertUpdateDataChange();
      PrimaryKey primaryKey = table.getPrimaryKey();
      if (primaryKey == null) {
        throw new IllegalArgumentException("No primary key found in " + table.getName());
      }
      change.setPrimaryKey(primaryKey.getColumnNames());
      return change;
    } else {
      return new InsertDataChange();
    }
  }

  private LoadDataChange newLoadDataChange(Table table, boolean insertUpdatePreferred) {
    if (insertUpdatePreferred) {
      LoadUpdateDataChange change = new LoadUpdateDataChange();
      PrimaryKey primaryKey = table.getPrimaryKey();
      if (primaryKey == null) {
        throw new IllegalArgumentException("No primary key found in " + table.getName());
      }
      change.setPrimaryKey(primaryKey.getColumnNames());
      return change;
    } else {
      return new LoadDataChange();
    }
  }

  protected ChangeSet generateChangeSet(Change change) {
    String id = null;
    if (change instanceof CreateTableChange) {
      id = ((CreateTableChange) change).getTableName();
    } else if (change instanceof AddPrimaryKeyChange) {
      id = ((AddPrimaryKeyChange) change).getConstraintName();
    } else if (change instanceof AddForeignKeyConstraintChange) {
      id = ((AddForeignKeyConstraintChange) change).getConstraintName();
    } else if (change instanceof AddUniqueConstraintChange) {
      id = ((AddUniqueConstraintChange) change).getConstraintName();
    } else if (change instanceof CreateIndexChange) {
      id = ((CreateIndexChange) change).getIndexName();
    } else if (change instanceof CreateViewChange) {
      id = ((CreateViewChange) change).getViewName();
    } else if (change instanceof CreateSequenceChange) {
      id = ((CreateSequenceChange) change).getSequenceName();
    }

    ChangeSet changeSet = ChangeSetUtils.generateChangeSet(id);
    changeSet.addChange(change);

    return changeSet;
  }

}
