package tools.sqlclient.lsp.pg;

import java.util.ArrayList;
import java.util.List;

/**
 * 将 LSP 的 (line,character) 与 Swing 文本 offset 互转。
 */
public class OffsetMapper {
    private final List<Integer> lineStarts = new ArrayList<>();

    public OffsetMapper(String text) {
        rebuild(text);
    }

    public final void rebuild(String text) {
        lineStarts.clear();
        lineStarts.add(0);
        if (text == null) {
            return;
        }
        int len = text.length();
        for (int i = 0; i < len; i++) {
            char c = text.charAt(i);
            if (c == '\n') {
                lineStarts.add(i + 1);
            }
        }
    }

    public int toOffset(int line, int character, int fallbackMax) {
        if (line < 0) return 0;
        if (line >= lineStarts.size()) {
            return Math.min(fallbackMax, fallbackMax);
        }
        int start = lineStarts.get(line);
        return Math.min(start + Math.max(0, character), fallbackMax);
    }
}
