package tools.sqlclient.lsp.pg;

import org.eclipse.lsp4j.Diagnostic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 缓存每个文档的诊断信息，供 UI 使用。
 */
public class DiagnosticsStore {
    private final Map<String, List<Diagnostic>> diagnostics = new HashMap<>();
    private final List<BiConsumer<String, List<Diagnostic>>> listeners = new ArrayList<>();

    public synchronized void update(String uri, List<Diagnostic> items) {
        diagnostics.put(uri, items == null ? List.of() : new ArrayList<>(items));
        notifyListeners(uri);
    }

    public synchronized List<Diagnostic> get(String uri) {
        return diagnostics.getOrDefault(uri, Collections.emptyList());
    }

    public synchronized void addListener(BiConsumer<String, List<Diagnostic>> listener) {
        listeners.add(listener);
    }

    private void notifyListeners(String uri) {
        List<Diagnostic> copy = get(uri);
        for (BiConsumer<String, List<Diagnostic>> listener : listeners) {
            listener.accept(uri, copy);
        }
    }
}
