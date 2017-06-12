package org.liquibase.maven.plugins;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.diff.output.DataInterceptor;
import liquibase.diff.output.DiffOutputControl;
import liquibase.diff.output.EbaoDiffOutputControl;
import liquibase.exception.DatabaseException;
import liquibase.exception.LiquibaseException;
import liquibase.executor.Executor;
import liquibase.executor.ExecutorService;
import liquibase.integration.commandline.CommandLineUtils;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.resource.ResourceAccessor;
import liquibase.statement.SqlStatement;
import liquibase.statement.core.RawSqlStatement;
import liquibase.util.ISODateFormat;
import liquibase.util.StreamUtil;
import liquibase.util.StringUtils;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugin.MojoExecutionException;

import com.ebao.tool.liquibase.util.LinkedProperties;
import com.ebao.tool.liquibase.util.PropertyParser;
import com.ebao.tool.liquibase.util.PropertyPath;

/**
 * Generates SQL that marks all unapplied changes as applied.
 *
 * @author Marcello Teodori
 * @goal generateChangeLog
 * @since 2.0.6
 */
public class LiquibaseGenerateChangeLogMojo extends
        AbstractLiquibaseMojo {

    private final Logger log = LogFactory.getInstance().getLog();

    /**
     * List of diff types to include in Change Log expressed as a comma separated list from: tables, views, columns, indexes, foreignkeys, primarykeys, uniqueconstraints, data.
     * If this is null then the default types will be: tables, views, columns, indexes, foreignkeys, primarykeys, uniqueconstraints
     *
     * @parameter expression="${liquibase.diffTypes}"
     */
    private String diffTypes;

    /**
     * Directory where insert statement csv files will be kept.
     *
     * @parameter expression="${liquibase.dataDir}"
     */
    private String dataDir;

    /**
     * The author to be specified for Change Sets in the generated Change Log.
     *
     * @parameter expression="${liquibase.changeSetAuthor}"
     */
    private String changeSetAuthor;

    /**
     * are required. If no context is specified then ALL contexts will be executed.
     * @parameter expression="${liquibase.contexts}" default-value=""
     */
    protected String contexts;

    /**
     * The execution context to be used for Change Sets in the generated Change Log, which can be "," separated if multiple contexts.
     *
     * @parameter expression="${liquibase.changeSetContext}"
     */
    private String changeSetContext;

    /**
     * The target change log file to output to. If this is null then the output will be to the screen.
     *
     * @parameter expression="${liquibase.outputChangeLogFile}"
     */
    protected String outputChangeLogFile;

  /**
   * @parameter expression="${liquibase.diffTable}"
   */
  protected String diffTable;

  /**
   * @parameter expression="${liquibase.diffCondition}"
   */
  protected String diffCondition;

  /**
   * @parameter expression="${liquibase.diffPropertyFile}"
   */
  protected String diffPropertyFile;

  /**
   * @parameter expression="${liquibase.diffPropertyString}"
   */
  protected String diffPropertyString;

  /**
   * @parameter expression="${liquibase.diffParameter}"
   */
  protected String diffParameter;

  /**
   * @parameter expression="${liquibase.userColumnPropertyFile}" default-value="liquibase/diff/output/ls.properties"
   */
  protected String userColumnPropertyFile;

  /**
   * @parameter expression="${liquibase.insertUpdate}" default-value="false"
   */
  protected boolean insertUpdate = false;

  /**
   * @parameter expression="${liquibase.metadataCachePreferred}" default-value="false"
   */
  protected boolean metadataCachePreferred = false;

  /**
   * @parameter expression="${liquibase.xmlCsvRowLimit}" default-value="1000"
   */
  protected int xmlCsvRowLimit = 1000;
  
  /**
   * @parameter expression="${liquibase.exportDateFormat}" default-value="yyyyMMddHHmmss";
   */
  protected String exportDateFormat = "yyyyMMddHHmmss";
  
	@Override
	protected void performLiquibaseTask(Liquibase liquibase)
			throws LiquibaseException {

        ClassLoader cl = null;
        try {
            cl = getClassLoaderIncludingProjectClasspath();
            Thread.currentThread().setContextClassLoader(cl);
        }
        catch (MojoExecutionException e) {
            throw new LiquibaseException("Could not create the class loader, " + e, e);
        }

        Database database = liquibase.getDatabase();
        System.setProperty(ISODateFormat.class.getName(), exportDateFormat);
        getLog().info("Generating Change Log from database " + database.toString());
        try {
            DiffOutputControl diffOutputControl = loadDiffProperty(liquibase);
            try {
                ResourceAccessor fo = getPropertyFileOpener(getMavenArtifactClassLoader());
                InputStream in = StreamUtil.singleInputStream(userColumnPropertyFile, fo);
                DataInterceptor.initUserColumnProperty(in);
            } catch (Exception e) {
                throw new IllegalArgumentException(userColumnPropertyFile, e);
            }
            CommandLineUtils.doGenerateChangeLog(outputChangeLogFile, database, defaultCatalogName, defaultSchemaName, StringUtils.trimToNull(diffTypes),
                    StringUtils.trimToNull(changeSetAuthor), StringUtils.trimToNull(changeSetContext), StringUtils.trimToNull(dataDir), diffOutputControl);
            getLog().info("Output written to Change Log file, " + outputChangeLogFile);
        }
        catch (IOException e) {
            throw new LiquibaseException(e);
        }
        catch (ParserConfigurationException e) {
            throw new LiquibaseException(e);
        }
	}

    private DiffOutputControl loadDiffProperty(Liquibase liquibase) {
        log.info("loading " + diffTypes + " from schema '" + defaultSchemaName + "'");
        Liquibase.setMetadataCachePreferred(metadataCachePreferred);
        EbaoDiffOutputControl diffControl = new EbaoDiffOutputControl(outputDefaultCatalog, outputDefaultSchema, true, liquibase.getDatabase());
        diffControl.setInsertUpdatePreferred(insertUpdate);
        diffControl.setXmlCsvRowLimit(xmlCsvRowLimit);
        if (outputChangeLogFile == null) {
			throw new IllegalArgumentException("outputChangeLogFile not set");
		}
        String dataDir = CommandLineUtils.createParentDir(outputChangeLogFile);
        diffControl.setDataDir(dataDir);

        if (diffTable != null) {
            diffTable = diffTable.toUpperCase().trim();
            diffControl.addDiffTable(diffTable, diffCondition);
            log.info("table to be exported is " + diffTable + "[" + diffCondition + "]");
        }

        Map<String, String> params = new HashMap<String, String>();
        if (diffParameter != null) {
            log.info("loading configuration with parameters[" + diffParameter + "]");
            String[] paramValues = diffParameter.split("[,;]");// the separator is either , or ;
            for (String pv : paramValues) {
                int index = pv.indexOf("=");
                if (index < 0) {
                    throw new IllegalArgumentException(diffParameter);
                }
                String param = pv.substring(0, index);
                String value = pv.substring(index + 1);
                if (value.startsWith("sql:")) {
                    Database database = liquibase.getDatabase();
                    Executor executor = ExecutorService.getInstance().getExecutor(database);
                    SqlStatement statement = new RawSqlStatement(value.substring("sql:".length()));
                    try {
                        value = (String) executor.queryForObject(statement, String.class);
                    } catch (DatabaseException e) {
                        throw new RuntimeException(e);
                    }
                }

                params.put(":" + param, value);
            }
        }

        if (diffPropertyFile != null && !"".equals(diffPropertyFile)) {
            String[] split = diffPropertyFile.split("[,;]");
            for (String propertyFile : split) {
                propertyFile = propertyFile.trim();
                loadDiffPropertyFile(propertyFile, params, diffControl);
            }
        }
        
        if (diffPropertyString != null && !"".equals(diffPropertyString)) {
          loadDiffPropertyString(diffPropertyString, params, diffControl);
        }

        return diffControl;
    }

    private void loadDiffPropertyFile(String propertyFile, Map<String, String> params, EbaoDiffOutputControl diffControl) {
        log.info("loading " + propertyFile + " with parameters[" + params + "]");

        Properties props = new LinkedProperties();
        InputStream in = null;
        try {
            ResourceAccessor fo = getPropertyFileOpener(getMavenArtifactClassLoader());
            in = StreamUtil.singleInputStream(propertyFile, fo);
            if (in == null) {
              throw new FileNotFoundException(propertyFile);
            }
            props.load(in);
        } catch (Exception e) {
            throw new IllegalArgumentException(propertyFile, e);
        } finally {
            IOUtils.closeQuietly(in);
        }

        loadDiffProperties(props, params, diffControl);
    }

    private void loadDiffPropertyString(String s, Map<String, String> params, EbaoDiffOutputControl diffControl) {
      log.info("loading\n" + s + "\nwith parameters[" + params + "]");

      StringReader r = new StringReader(s);
      Properties props = new LinkedProperties();
      try {
          props.load(r);
      } catch (Exception e) {
          throw new IllegalArgumentException(propertyFile, e);
      } finally {
          IOUtils.closeQuietly(r);
      }
      
      loadDiffProperties(props, params, diffControl);
    }
    
    private void loadDiffProperties(Properties props, Map<String, String> params,
        EbaoDiffOutputControl diffControl) {
      PropertyParser parser = new PropertyParser();
      for (Object key : props.keySet()) {
          if ("".equals(key)) {
              throw new IllegalArgumentException("empty property name");
          }

          String condition = (String) props.get(key);
          for (String paramName : params.keySet()) {
              String paramValue = params.get(paramName);
              key = ((String)key).replaceAll(paramName, paramValue);
              condition = condition.replaceAll(paramName, paramValue);
          }

          PropertyPath path = parser.parseProperty((String) key);
          diffControl.addDiffTable(path.table, condition, path.dir, path.filename);
          log.info("table to be exported " + path.table + "[" + condition + "]");
      }
    }
	
	@Override
	protected void printSettings(String indent) {
		super.printSettings(indent);
        getLog().info(indent + "diffTypes: " + diffTypes);
        getLog().info(indent + "outputChangeLogFile: " + outputChangeLogFile);
        if (dataDir != null) {
        	getLog().info(indent + "dataDir: " + dataDir);
		}
        getLog().info(indent + "userColumnPropertyFile: " + userColumnPropertyFile);
        if (insertUpdate) {
          getLog().info(indent + "insertUpdate: " + insertUpdate);
        }
        if (xmlCsvRowLimit != 1000) {
        	getLog().info(indent + "xmlCsvRowLimit: " + xmlCsvRowLimit);
		}
	}

}
