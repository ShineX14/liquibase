package liquibase.parser.core.xml;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.validation.SchemaFactory;

import liquibase.changelog.ChangeLogParameters;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.ChangeLogParseException;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import liquibase.parser.core.ParsedNode;
import liquibase.resource.UtfBomStripperInputStream;
import liquibase.resource.ResourceAccessor;
import liquibase.util.StreamUtil;
import liquibase.util.file.FilenameUtils;

import org.apache.commons.io.IOUtils;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.SAXParseException;
import org.xml.sax.XMLReader;

public class XMLChangeLogSAXParser extends AbstractChangeLogParser {

	private Logger logger=LogFactory.getInstance().getLog();
    protected SAXParserFactory saxParserFactory;

    public XMLChangeLogSAXParser() {
        saxParserFactory = SAXParserFactory.newInstance();
        saxParserFactory.setValidating(true);
        saxParserFactory.setNamespaceAware(true);
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    public static String getSchemaVersion() {
        return "3.3";
    }

    @Override
    public boolean supports(String changeLogFile, ResourceAccessor resourceAccessor) {
        return changeLogFile.toLowerCase().endsWith("xml");
    }

    protected SAXParserFactory getSaxParserFactory() {
        return saxParserFactory;
    }

    @Override
    protected ParsedNode parseToNode(DatabaseChangeLog changeLog, ChangeLogParameters changeLogParameters, ResourceAccessor resourceAccessor) throws ChangeLogParseException {
        String physicalChangeLogLocation = changeLog.getPhysicalFilePath();
        InputStream inputStream = null;
        try {
            SAXParser parser = saxParserFactory.newSAXParser();
            try {
                parser.setProperty("http://java.sun.com/xml/jaxp/properties/schemaLanguage", "http://www.w3.org/2001/XMLSchema");
            } catch (SAXNotRecognizedException e) {
                //ok, parser must not support it
            } catch (SAXNotSupportedException e) {
                //ok, parser must not support it
            }

            XMLReader xmlReader = parser.getXMLReader();
            LiquibaseEntityResolver resolver=new LiquibaseEntityResolver(this);
            resolver.useResoureAccessor(resourceAccessor,FilenameUtils.getFullPath(physicalChangeLogLocation));
            xmlReader.setEntityResolver(resolver);
            xmlReader.setErrorHandler(new ErrorHandler() {
                @Override
                public void warning(SAXParseException exception) throws SAXException {
                    LogFactory.getLogger().warning(exception.getMessage());
                    throw exception;
                }

                @Override
                public void error(SAXParseException exception) throws SAXException {
                    LogFactory.getLogger().severe(exception.getMessage());
                    throw exception;
                }

                @Override
                public void fatalError(SAXParseException exception) throws SAXException {
                    LogFactory.getLogger().severe(exception.getMessage());
                    throw exception;
                }
            });
        	
            inputStream = StreamUtil.singleInputStream(physicalChangeLogLocation, resourceAccessor);
            if (inputStream == null) {
                throw new ChangeLogParseException(physicalChangeLogLocation + " does not exist");
            }

            XMLChangeLogSAXHandler contentHandler = new XMLChangeLogSAXHandler(changeLog, resourceAccessor, changeLogParameters);
            xmlReader.setContentHandler(contentHandler);
            xmlReader.parse(new InputSource(new UtfBomStripperInputStream(inputStream)));

            return contentHandler.getDatabaseChangeLogTree();
        } catch (ChangeLogParseException e) {
            throw e;
        } catch (IOException e) {
            throw new ChangeLogParseException("Error Reading Migration File " + physicalChangeLogLocation + ": " + e.getMessage(), e);
        } catch (SAXParseException e) {
            throw new ChangeLogParseException("Error parsing line " + e.getLineNumber() + " column " + e.getColumnNumber() + " in " + physicalChangeLogLocation +": " + e.getMessage(), e);
        } catch (SAXException e) {
            Throwable parentCause = e.getException();
            while (parentCause != null) {
                if (parentCause instanceof ChangeLogParseException) {
                    throw ((ChangeLogParseException) parentCause);
                }
                parentCause = parentCause.getCause();
            }
            String reason = e.getMessage();
            if (reason == null) {
                if (e.getCause() != null) {
                    reason = e.getCause().getMessage();
                }
            }
            if (reason == null) {
                reason = "Unknown Reason";
            }

            throw new ChangeLogParseException("Invalid Migration File " + physicalChangeLogLocation + ": " + reason, e);
        } catch (Exception e) {
            throw new ChangeLogParseException(physicalChangeLogLocation, e);
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }
}
