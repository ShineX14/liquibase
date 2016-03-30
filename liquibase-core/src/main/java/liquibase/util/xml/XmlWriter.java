package liquibase.util.xml;

import org.w3c.dom.Document;

import java.io.IOException;
import java.io.OutputStream;

public interface XmlWriter {
    int INDENT_NUMBER = 2;
    public void write(Document doc, OutputStream outputStream) throws IOException;
}
