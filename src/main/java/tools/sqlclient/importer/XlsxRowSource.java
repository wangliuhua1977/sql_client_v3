package tools.sqlclient.importer;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.usermodel.XSSFComment;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * 使用 POI SAX 事件流式读取 XLSX，保持列对齐。
 */
public class XlsxRowSource implements RowSource {
    private final Path xlsxPath;
    private final String sheetName;
    private OPCPackage pkg;
    private ReadOnlySharedStringsTable strings;
    private XSSFReader reader;
    private XMLReader parser;
    private final Deque<RowData> rowBuffer = new ArrayDeque<>();
    private List<String> headers;
    private long lineNumber;

    public XlsxRowSource(Path xlsxPath, String sheetName) {
        this.xlsxPath = xlsxPath;
        this.sheetName = sheetName;
    }

    @Override
    public void open() {
        try {
            this.pkg = OPCPackage.open(xlsxPath.toFile());
            this.strings = new ReadOnlySharedStringsTable(pkg);
            this.reader = new XSSFReader(pkg);
            DataFormatter formatter = new DataFormatter();
            this.parser = XMLReaderFactory.createXMLReader();
            this.parser.setContentHandler(new SheetHandler(strings, formatter, rowBuffer));
            InputStream sheetStream = resolveSheetStream();
            parser.parse(new InputSource(sheetStream));
            this.lineNumber = 0;
            if (!rowBuffer.isEmpty()) {
                headers = rowBuffer.removeFirst().getValues();
            }
        } catch (Exception e) {
            throw new RuntimeException("读取 XLSX 失败: " + e.getMessage(), e);
        }
    }

    private InputStream resolveSheetStream() throws Exception {
        XSSFReader.SheetIterator it = (XSSFReader.SheetIterator) reader.getSheetsData();
        while (it.hasNext()) {
            InputStream stream = it.next();
            String name = it.getSheetName();
            if (sheetName == null || sheetName.isBlank() || name.equals(sheetName)) {
                return stream;
            }
        }
        throw new IllegalArgumentException("未找到 Sheet: " + sheetName);
    }

    @Override
    public List<String> getHeaders() {
        return headers == null ? List.of() : headers;
    }

    @Override
    public RowData nextRow() {
        if (headers == null) {
            return null;
        }
        if (rowBuffer.isEmpty()) {
            return null;
        }
        RowData next = rowBuffer.removeFirst();
        lineNumber++;
        if (next.getValues().size() != headers.size()) {
            throw new IllegalStateException("列数不一致: 期望 " + headers.size() + " 实际 " + next.getValues().size());
        }
        return new RowData(lineNumber, next.getValues());
    }

    @Override
    public void close() {
        try {
            if (pkg != null) {
                pkg.close();
            }
        } catch (Exception ignored) {
        }
    }

    private static class SheetHandler extends org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler {
        SheetHandler(ReadOnlySharedStringsTable sst, DataFormatter formatter, Deque<RowData> buffer) {
            super(null, null, sst, new BufferingHandler(buffer), formatter, false);
        }
    }

    private static class BufferingHandler implements org.apache.poi.xssf.eventusermodel.XSSFSheetXMLHandler.SheetContentsHandler {
        private final Deque<RowData> buffer;
        private List<String> current;
        private long rowNum;

        BufferingHandler(Deque<RowData> buffer) {
            this.buffer = buffer;
        }

        @Override
        public void startRow(int rowNum) {
            this.rowNum = rowNum;
            this.current = new ArrayList<>();
        }

        @Override
        public void endRow(int rowNum) {
            buffer.add(new RowData(this.rowNum, current));
        }

        @Override
        public void cell(String cellReference, String formattedValue, XSSFComment comment) {
            int colIndex = getColumnIndex(cellReference);
            while (current.size() < colIndex) {
                current.add("");
            }
            current.add(formattedValue == null ? "" : formattedValue);
        }

        @Override
        public void headerFooter(String text, boolean isHeader, String tagName) {
            // ignore
        }

        private int getColumnIndex(String cellReference) {
            int idx = 0;
            for (int i = 0; i < cellReference.length(); i++) {
                char c = cellReference.charAt(i);
                if (Character.isLetter(c)) {
                    idx = idx * 26 + (c - 'A' + 1);
                } else {
                    break;
                }
            }
            return idx;
        }
    }
}
