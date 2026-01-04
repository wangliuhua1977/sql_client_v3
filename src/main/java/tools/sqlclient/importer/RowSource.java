package tools.sqlclient.importer;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 可流式读取行的来源定义。
 */
public interface RowSource extends Closeable {
    void open() throws IOException;

    List<String> getHeaders();

    /**
     * 读取下一行数据，返回 null 表示结束。
     */
    RowData nextRow() throws IOException;

    @Override
    void close() throws IOException;
}
