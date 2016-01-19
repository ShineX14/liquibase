package liquibase.resource;

import liquibase.logging.Logger;
import liquibase.logging.core.DefaultLogger;
import liquibase.util.FileUtil;
import liquibase.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.*;

/**
 * An implementation of {@link liquibase.resource.ResourceAccessor} that wraps a class loader.
 */
public class ClassLoaderResourceAccessor extends AbstractResourceAccessor {

    private static final Logger logger = new DefaultLogger();
    
    private ClassLoader classLoader;
    public ClassLoaderResourceAccessor() {
        this.classLoader = getClass().getClassLoader();
        init(); //init needs to be called after classloader is set
    }

    public ClassLoaderResourceAccessor(ClassLoader classLoader) {
        this.classLoader = classLoader;
        init(); //init needs to be called after classloader is set
    }

    private Set<InputStream> getResourcesAsStream(String path, boolean single) throws IOException {
        Enumeration<URL> resources = classLoader.getResources(path);
        if (resources == null || !resources.hasMoreElements()) {
            return null;
        }
        List<String> seenUrls = new ArrayList<String>();
        List<InputStream> returnSet = new ArrayList<InputStream>();
        while (resources.hasMoreElements()) {
            URL url = resources.nextElement();
            if (seenUrls.contains(url.toExternalForm())) {
                continue;
            }
            seenUrls.add(url.toExternalForm());
            InputStream resourceAsStream = url.openStream();
            if (resourceAsStream == null) {
            	throw new IllegalArgumentException(url.toExternalForm());
            }
            returnSet.add(resourceAsStream);
        }

        if (seenUrls.size() > 1 && !"META-INF/MANIFEST.MF".equals(path)) {
            logger.warning(seenUrls.toString());
        }
        Set<InputStream> set = new HashSet<InputStream>();
        if (single && returnSet.size() > 1) {
        	if (returnSet.size() > 2) {
				throw new IllegalArgumentException(returnSet.size() + " resources are found for " +  path);
			}
        	
        	String url1 = seenUrls.get(0);
        	String url2 = seenUrls.get(1);
        	if (url1.startsWith("file:") && url2.startsWith("jar:")) {
				set.add(returnSet.get(0));
			} else {
        	    throw new IllegalArgumentException(returnSet.size() + " resources are found for " +  path);
			}
		} else {
            set.addAll(returnSet);
		}
        return set;
    }

    @Override
    public Set<InputStream> getResourcesAsStream(String path) throws IOException {
    	return getResourcesAsStream(path, false);
    }
    
	@Override
	public InputStream getSingleResourceAsStream(String path) throws IOException {
		Set<InputStream> set = getResourcesAsStream(path, true);
		if (set == null || set.size() == 0) {
			return null;
		} else if (set.size() > 1) {
			throw new IllegalArgumentException(set.size() + " resources are found for " + path);
		}
		return set.iterator().next();
	}

    @Override
    public Set<String> list(String relativeTo, String path, boolean includeFiles, boolean includeDirectories, boolean recursive) throws IOException {
        path = convertToPath(relativeTo, path);

        URL fileUrl = classLoader.getResource(path);
        if (fileUrl == null) {
            return null;
        }

        if (!fileUrl.toExternalForm().startsWith("file:")) {
            if (fileUrl.toExternalForm().startsWith("jar:file:")
                    || fileUrl.toExternalForm().startsWith("wsjar:file:")
                    || fileUrl.toExternalForm().startsWith("zip:")) {

                String file = fileUrl.getFile();
                String splitPath = file.split("!")[0];
                if (splitPath.matches("file:\\/[A-Za-z]:\\/.*")) {
                    splitPath = splitPath.replaceFirst("file:\\/", "");
                } else {
                    splitPath = splitPath.replaceFirst("file:", "");
                }
                splitPath = URLDecoder.decode(splitPath, "UTF-8");
                File zipfile = new File(splitPath);


                File zipFileDir = FileUtil.unzip(zipfile);
                if (path.startsWith("classpath:")) {
                    path = path.replaceFirst("classpath:", "");
                }
                if (path.startsWith("classpath*:")) {
                    path = path.replaceFirst("classpath\\*:", "");
                }
                URI fileUri = new File(zipFileDir, path).toURI();
                fileUrl = fileUri.toURL();
            }
        }

        try {
            File file = new File(fileUrl.toURI());
            if (file.exists()) {
                Set<String> returnSet = new HashSet<String>();
                getContents(file, recursive, includeFiles, includeDirectories, path, returnSet);
                return returnSet;
            }
        } catch (URISyntaxException e) {
            //not a local file
        } catch (IllegalArgumentException e) {
            //not a local file
        }

        Enumeration<URL> resources = classLoader.getResources(path);
        if (resources == null || !resources.hasMoreElements()) {
            return null;
        }
        Set<String> returnSet = new HashSet<String>();
        while (resources.hasMoreElements()) {
            String url = resources.nextElement().toExternalForm();
            url = url.replaceFirst("^\\Q"+path+"\\E", "");
            returnSet.add(url);
        }
        return returnSet;
    }

    @Override
    public ClassLoader toClassLoader() {
        return classLoader;
    }

    @Override
    public String toString() {
        String description;
        if (classLoader instanceof URLClassLoader) {
            List<String> urls = new ArrayList<String>();
            for (URL url : ((URLClassLoader) classLoader).getURLs()) {
                urls.add(url.toExternalForm());
            }
            description = StringUtils.join(urls, ",");
        } else {
            description = classLoader.getClass().getName();
        }
        return getClass().getName()+"("+ description +")";

    }
}
