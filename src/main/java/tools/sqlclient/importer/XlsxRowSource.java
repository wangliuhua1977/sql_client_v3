package tools.sqlclient.importer;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class XlsxRowSource implements RowSource {
    private final OPCPackage pkg;
    private final ReadOnlySharedStringsTable sst;
    private final XMLReader parser;
    private final Queue<List<String>> rowQueue = new LinkedList<>();
    private final InputStream sheetInputStream;

    public XlsxRowSource(String path) throws Exception {
        this.pkg = OPCPackage.open(new FileInputStream(path));
        this.sst = new ReadOnlySharedStringsTable(pkg);
        XSSFReader reader = new XSSFReader(pkg);
        this.parser = XMLReaderFactory.createXMLReader();
        this.parser.setContentHandler(new SheetHandler());
        this.sheetInputStream = reader.getSheetsData().next();
        parser.parse(new InputSource(sheetInputStream));
    }

    @Override
    public Iterator<List<String>> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return !rowQueue.isEmpty();
            }

            @Override
            public List<String> next() {
                if (rowQueue.isEmpty()) {
                    throw new NoSuchElementException();
                }
                return rowQueue.poll();
            }
        };
    }

    @Override
    public void close() throws IOException {
        try {
            sheetInputStream.close();
        } finally {
            pkg.revert();
        }
    }

    private class SheetHandler extends DefaultHandler {
        private String lastContents;
        private boolean nextIsString;
        private final List<String> currentRow = new ArrayList<>();

        @Override
        public void startElement(String uri, String localName, String name, Attributes attributes) throws SAXException {
            if (name.equals("c")) {
                String cellType = attributes.getValue("t");
                nextIsString = cellType != null && cellType.equals("s");
            }
            lastContents = "";
        }

        @Override
        public void endElement(String uri, String localName, String name) throws SAXException {
            if (nextIsString && name.equals("v")) {
                int idx = Integer.parseInt(lastContents);
                lastContents = sst.getItemAt(idx).getString();
            }
            if (name.equals("v")) {
                currentRow.add(lastContents);
            }
            if (name.equals("row")) {
                rowQueue.add(new ArrayList<>(currentRow));
                currentRow.clear();
            }
        }

        @Override
        public void characters(char[] ch, int start, int length) throws SAXException {
            lastContents += new String(ch, start, length);
        }
    }
}
