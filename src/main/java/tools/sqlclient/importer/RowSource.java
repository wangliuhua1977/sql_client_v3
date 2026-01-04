package tools.sqlclient.importer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public interface RowSource extends Closeable, Iterable<List<String>> {
    @Override
    Iterator<List<String>> iterator();

    @Override
    void close() throws IOException;

    default String name() {
        return getClass().getSimpleName();
    }
}
