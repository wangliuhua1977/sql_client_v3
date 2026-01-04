package tools.sqlclient.importer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

public final class RowSourceFactory {
    private RowSourceFactory() {
    }

    public static RowSource fromClipboard(String rawText) {
        return new ClipboardRowSource(rawText);
    }

    public static RowSource fromCsv(Path path, Charset charset, char delimiter, boolean hasHeader) throws IOException {
        return new CsvRowSource(path, charset, delimiter, hasHeader);
    }

    public static RowSource fromXlsx(Path path, String sheetName, int headerRow, int dataStartRow) throws IOException {
        return new XlsxRowSource(path, sheetName, headerRow, dataStartRow);
    }
}
