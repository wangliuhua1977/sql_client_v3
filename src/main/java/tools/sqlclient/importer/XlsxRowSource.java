package tools.sqlclient.importer;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.apache.poi.xssf.usermodel.helpers.XSSFSheetXMLHandler;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Simplified SAX-based XLSX reader. Uses event handler to avoid loading full workbook.
 */
public class XlsxRowSource implements RowSource {
    private final Path path;
    private final String sheetName;
    private final int headerRowIndex;
    private final int dataStartRowIndex;
    private List<String> header = new ArrayList<>();
    private List<List<String>> rows = new ArrayList<>();
    private Iterator<List<String>> iterator;

    public XlsxRowSource(Path path, String sheetName, int headerRowIndex, int dataStartRowIndex) {
        this.path = path;
        this.sheetName = sheetName;
        this.headerRowIndex = headerRowIndex;
        this.dataStartRowIndex = dataStartRowIndex;
        load();
    }

    private void load() {
        rows.clear();
        header = new ArrayList<>();
        try (OPCPackage pkg = OPCPackage.open(path.toFile())) {
            ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(pkg);
            XSSFReader reader = new XSSFReader(pkg);
            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator) reader.getSheetsData();
            InputStream sheetStream = null;
            while (iter.hasNext()) {
                InputStream stream = iter.next();
                String name = iter.getSheetName();
                if (sheetName == null || sheetName.isBlank() || sheetName.equals(name)) {
                    sheetStream = stream;
                    break;
                } else {
                    stream.close();
                }
            }
            if (sheetStream == null) {
                return;
            }
            DataFormatter formatter = new DataFormatter();
            XSSFSheetXMLHandler handler = new XSSFSheetXMLHandler(reader.getStylesTable(), null, strings, new SheetHandler(formatter), formatter, false);
            XMLReader xmlReader = XMLReaderFactory.createXMLReader();
            xmlReader.setContentHandler(handler);
            xmlReader.parse(new InputSource(sheetStream));
            iterator = rows.iterator();
        } catch (Exception e) {
            throw new RuntimeException("读取 XLSX 失败: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getHeader() {
        return header;
    }

    @Override
    public List<String> nextRow() {
        if (iterator != null && iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public RowSource reopen() {
        load();
        return this;
    }

    @Override
    public String getDescription() {
        return "XLSX:" + path.toAbsolutePath();
    }

    @Override
    public void close() {
        // nothing
    }

    private class SheetHandler implements XSSFSheetXMLHandler.SheetContentsHandler {
        private final DataFormatter formatter;
        private List<String> current;
        private int currentRow = -1;
        private int maxCol = 0;

        SheetHandler(DataFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public void startRow(int rowNum) {
            currentRow = rowNum;
            current = new ArrayList<>();
        }

        @Override
        public void endRow(int rowNum) {
            if (current == null) {
                return;
            }
            while (current.size() < maxCol) {
                current.add("");
            }
            if (rowNum == headerRowIndex) {
                header = new ArrayList<>(current);
            } else if (rowNum >= dataStartRowIndex) {
                rows.add(new ArrayList<>(current));
            }
        }

        @Override
        public void cell(String cellReference, String formattedValue, XSSFComment comment) {
            String value = formattedValue == null ? "" : formattedValue;
            current.add(value);
            if (current.size() > maxCol) {
                maxCol = current.size();
            }
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
        }
    }
}
