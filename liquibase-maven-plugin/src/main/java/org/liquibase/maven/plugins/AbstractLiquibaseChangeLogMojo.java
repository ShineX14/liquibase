// Version:   $Id: $
// Copyright: Copyright(c) 2007 Trace Financial Limited
package org.liquibase.maven.plugins;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.CompositeResourceAccessor;
import liquibase.resource.FileSystemResourceAccessor;
import liquibase.resource.ResourceAccessor;

import org.apache.commons.io.FileUtils;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * A Liquibase MOJO that requires the user to provide a DatabaseChangeLogFile to be able
 * to perform any actions on the database.
 * @author Peter Murray
 */
public abstract class AbstractLiquibaseChangeLogMojo extends AbstractLiquibaseMojo {

  /**
   * Specifies the change log file to use for Liquibase.
   * @parameter expression="${liquibase.changeLogFile}"
   */
  protected String changeLogFile;

  /**
   * Specifies the initialization change log file to use for Liquibase.
   * @parameter expression="${liquibase.changeLogFileBefore}"
   */
  protected String changeLogFileBefore;
  /**
   * Specifies the clean-up change log file to use for Liquibase.
   * @parameter expression="${liquibase.changeLogFileAfter}"
   */
  protected String changeLogFileAfter;

  /**
   * @parameter expression="${liquibase.relativeToChangelogFile}"
   */
  protected String relativeToChangelogFile;

  /**
   * The Liquibase contexts to execute, which can be "," separated if multiple contexts
   * are required. If no context is specified then ALL contexts will be executed.
   * @parameter expression="${liquibase.contexts}" default-value=""
   */
  protected String contexts;

    /**
     * The Liquibase labels to execute, which can be "," separated if multiple labels
     * are required or a more complex expression. If no label is specified then ALL all will be executed.
     * @parameter expression="${liquibase.labels}" default-value=""
     */
  protected String labels;

  /**
   * @parameter expression="${liquibase.resourceJars}" default-value=""
   */
  protected String resourceJars;

  /**
   * @parameter expression="${liquibase.resourceJarPath}" default-value=""
   */
  protected String resourceJarPath;

  /**
   * @parameter expression="${liquibase.localFileFirst}" default-value="false"
   */
  protected boolean localFileFirst = false;
  
  @Override
  protected void checkRequiredParametersAreSpecified() throws MojoFailureException {
    super.checkRequiredParametersAreSpecified();

    if (changeLogFile == null) {
      throw new MojoFailureException("The changeLogFile must be specified.");
    }
  }

  /**
   * Performs the actual Liquibase task on the database using the fully configured {@link
   * liquibase.Liquibase}.
   * @param liquibase The {@link liquibase.Liquibase} that has been fully
   * configured to run the desired database task.
   */
  @Override
  protected void performLiquibaseTask(Liquibase liquibase) throws LiquibaseException {
  }

  @Override
  protected void printSettings(String indent) {
    super.printSettings(indent);
    if (changeLogFileBefore != null && changeLogFileBefore.length() > 0) {
        getLog().info(indent + "changeLogFileBefore: " + changeLogFileBefore);
    }
    getLog().info(indent + "changeLogFile: " + changeLogFile);
    if (changeLogFileAfter != null && changeLogFileAfter.length() > 0) {
        getLog().info(indent + "changeLogFileAfter: " + changeLogFileAfter);
    }
    if (contexts != null && contexts.length() > 0) {
        getLog().info(indent + "context(s): " + contexts);
    }
    if (labels != null && labels.length() > 0) {
        getLog().info(indent + "label(s): " + labels);
    }
    if (localFileFirst) {
        getLog().info(indent + "localFileFirst: " + localFileFirst);
    }
  }

  private ResourceAccessor cachedFileOpener;
  
  @Override
  protected ResourceAccessor getFileOpener(ClassLoader cl) {
    if (cachedFileOpener != null) {
        return cachedFileOpener;
    }
    
    ResourceAccessor mFO = new MavenResourceAccessor(cl);
    ResourceAccessor fsFO = new FileSystemResourceAccessor(project.getBasedir().getAbsolutePath());
    //return new CompositeResourceAccessor(mFO, fsFO);
    try {
        if (localFileFirst) {
            cachedFileOpener = new CompositeResourceAccessor(getResourceJarsLoader(), fsFO, mFO);
        } else {
            cachedFileOpener = new CompositeResourceAccessor(getResourceJarsLoader(), mFO, fsFO);
        }
    } catch (MalformedURLException e) {
        throw new IllegalArgumentException(e);
    }
    return cachedFileOpener;
  }

  private static final String[] RESOURCE_FILE_EXT = new String[] { "jar", "zip" };

  private ResourceAccessor getResourceJarsLoader() throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();

    if (resourceJars != null) {
      getLog().info("resourceJar(s): " + resourceJars);
      String[] jars = resourceJars.split("[,;]");
      for (int i = 0; i < jars.length; i++) {
        File f = new File(jars[i]);
        if (!f.exists()) {
          throw new IllegalArgumentException(jars[i] + " not found");
        }
        addResourceJar(f, urls);
      }
      getLog().info("");
    }
    
    if (resourceJarPath != null) {
      getLog().info("resourceJarPath: " + resourceJarPath);
      String[] dirs = resourceJarPath.split("[,;]");
      for (int i = 0; i < dirs.length; i++) {
        File dir = new File(dirs[i]);
        if (!dir.exists()) {
          continue;
        }
        for (File jar : FileUtils.listFiles(dir, RESOURCE_FILE_EXT, true)) {
          addResourceJar(jar, urls);
        }
        getLog().info("");
      }
    }

    return new ClassLoaderResourceAccessor(new URLClassLoader(urls.toArray(new URL[urls.size()])));
  }

  private void addResourceJar(File f, List<URL> urls)
      throws MalformedURLException {
    URL url = f.toURI().toURL();
    urls.add(url);
    getLog().info("Resource:" + url.toString());
  }
  
  @Override
  protected Liquibase createLiquibase(ResourceAccessor fo, Database db) throws MojoExecutionException {
        try {
            String changeLog = changeLogFile == null ? "" : changeLogFile.trim();
            Liquibase liquibase = new Liquibase(changeLog, fo, db);
            liquibase.setChangeLogFileBefore(changeLogFileBefore);
            liquibase.setChangeLogFileAfter(changeLogFileAfter);
            if ("true".equalsIgnoreCase(relativeToChangelogFile)) {
              Liquibase.setRelativeToChangelogFile();
            }
            return liquibase;
        } catch (LiquibaseException ex) {
            throw new MojoExecutionException("Error creating liquibase: "+ex.getMessage(), ex);
        }
  }
}
