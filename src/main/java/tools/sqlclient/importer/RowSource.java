package tools.sqlclient.importer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Stream row iterator abstraction to avoid loading all data into memory.
 */
public interface RowSource extends Closeable {
    /**
     * @return header columns, never null.
     */
    List<String> getHeader();

    /**
     * Read next row. Returns null when no more rows.
     */
    List<String> nextRow() throws IOException;

    /**
     * Reset to beginning if supported.
     */
    default RowSource reopen() throws IOException {
        return this;
    }

    /**
     * @return human readable description for report.
     */
    String getDescription();

    @Override
    void close() throws IOException;
}
