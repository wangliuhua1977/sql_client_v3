package tools.sqlclient.importer;

import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * 创建 RowSource 的工厂。
 */
public final class RowSourceFactory {
    private RowSourceFactory() {
    }

    public static RowSource forClipboard(String text) {
        return new ClipboardRowSource(text);
    }

    public static RowSource forCsv(Path path, Charset charset, char delimiter, char quote) {
        return new CsvRowSource(path, charset, delimiter, quote);
    }

    public static RowSource forXlsx(Path path, String sheetName) {
        return new XlsxRowSource(path, sheetName);
    }
}
