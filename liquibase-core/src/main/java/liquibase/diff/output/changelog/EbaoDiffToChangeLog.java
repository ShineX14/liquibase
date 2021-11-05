package liquibase.diff.output.changelog;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.IncludedFile;
import liquibase.diff.DiffResult;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.serializer.ChangeLogSerializer;
import liquibase.serializer.ChangeLogSerializerFactory;

public class EbaoDiffToChangeLog extends DiffToChangeLog {

  private final Logger logger = LogFactory.getInstance().getLog();
  private final EbaoDiffOutputControl diffOutputControl;

  public EbaoDiffToChangeLog(DiffResult diffResult, DiffOutputControl diffOutputControl) {
    super(diffResult, diffOutputControl);
    this.diffOutputControl = (EbaoDiffOutputControl) diffOutputControl;
  }
  
  @Override
  public List<ChangeSet> generateChangeSets() {
    List<ChangeSet> changeSets = super.generateChangeSets();
    if (diffOutputControl.isIndexFileInEachDirectory()) {
      List<IncludedFile> includedFiles = new ArrayList<IncludedFile>();
      for (ChangeSet includedFile : changeSets) {
        includedFiles.add((IncludedFile)includedFile);
      }
      includedFiles = processIndexFileInEachDirectory(includedFiles, diffOutputControl.getDataDir());
      changeSets.clear();
      changeSets.addAll(includedFiles);
    }

    return changeSets;
  }

  private List<IncludedFile> processIndexFileInEachDirectory(List<IncludedFile> srcChanges, String baseDir) {
    List<IncludedFile> dstChanges = new ArrayList<IncludedFile>();
    Map<String, List<IncludedFile>> map = new HashMap<String, List<IncludedFile>>();
    for (IncludedFile includedFile : srcChanges) {
      String path = includedFile.getFileName();
      int i = path.lastIndexOf('/');
      if (i < 0) {
        dstChanges.add(includedFile);
        continue;
      }
      
      String dir = path.substring(0, i + 1);
      String name = path.substring(i+1);
      String indexFile = dir + "upgrade-db.xml";
      List<IncludedFile> list = map.get(indexFile);
      if (list == null) {
        list = new ArrayList<IncludedFile>();
        map.put(indexFile, list);

        IncludedFile includedIndexFile = new IncludedFile(indexFile, includedFile.getTableName());
        dstChanges.add(includedIndexFile);
      }
      list.add(new IncludedFile(name, includedFile.getTableName()));
    }
    
    generateIndexFileInEachDirectory(map, baseDir);
    
    return dstChanges;
  }

  private void generateIndexFileInEachDirectory(Map<String, List<IncludedFile>> map, String baseDir) {
    for (String indexFile : map.keySet()) {
      List<IncludedFile> list = map.get(indexFile);
      generateIndexFileInEachDirectory(indexFile, list, baseDir);
    }
  }

  private void generateIndexFileInEachDirectory(String indexFile, List<IncludedFile> list, String baseDir) {
    try {
      _generateIndexFileInEachDirectory(indexFile, list, baseDir);
    } catch (IOException e) {
      throw new RuntimeException(indexFile, e);
    }
  }
  
  private void _generateIndexFileInEachDirectory(String indexFile, List<IncludedFile> list, String baseDir)
      throws FileNotFoundException, IOException {
    File file = new File(baseDir, indexFile);
    if (file.exists()) {
      throw new IllegalStateException(indexFile + " exists in " + file.getAbsolutePath());
    }

    file.getParentFile().mkdirs();
    file.createNewFile();
    PrintStream output = null;
    try {
      output = new PrintStream(new FileOutputStream(file));
      ChangeLogSerializer changeLogSerializer = ChangeLogSerializerFactory.getInstance().getSerializer(indexFile);

      List<ChangeSet> changeSets = new ArrayList<ChangeSet>();
      changeSets.addAll(list);
      changeLogSerializer.write(changeSets, output);
      logger.info(indexFile);
    } finally {
      IOUtils.closeQuietly(output);
    }
  }
    
}
