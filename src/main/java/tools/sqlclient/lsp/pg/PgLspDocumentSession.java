package tools.sqlclient.lsp.pg;

import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.Range;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Highlighter;
import java.awt.*;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 管理单个 SQL Tab 的文档生命周期与诊断高亮。
 */
public class PgLspDocumentSession {
    private final RSyntaxTextArea textArea;
    private final PgLspClient client;
    private final DiagnosticsStore diagnosticsStore;
    private final String uri;
    private final AtomicInteger version = new AtomicInteger(1);
    private final Timer debounceTimer;
    private final Highlighter highlighter;
    private Object tooltipTag;
    private boolean opened;

    public PgLspDocumentSession(RSyntaxTextArea area, PgLspClient client, DiagnosticsStore store) {
        this.textArea = area;
        this.client = client;
        this.diagnosticsStore = store;
        this.uri = "memory:///" + UUID.randomUUID() + ".sql";
        this.highlighter = area.getHighlighter();
        this.debounceTimer = new Timer(380, e -> sendDidChange());
        this.debounceTimer.setRepeats(false);
        initListeners();
        ensureOpened();
    }

    public String getUri() {
        return uri;
    }

    private void initListeners() {
        textArea.getDocument().addDocumentListener(new tools.sqlclient.ui.SimpleDocumentListener(() -> {
            if (client == null || !client.isReady()) return;
            debounceTimer.restart();
        }));
        diagnosticsStore.addListener((u, list) -> {
            if (!uri.equals(u)) return;
            SwingUtilities.invokeLater(() -> renderDiagnostics(list));
        });
        textArea.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            @Override
            public void mouseMoved(java.awt.event.MouseEvent e) {
                showTooltip(e.getPoint());
            }
        });
    }

    public void ensureOpened() {
        if (opened || client == null || !client.isReady()) {
            return;
        }
        client.didOpen(uri, "sql", textArea.getText(), version.get());
        opened = true;
    }

    private void sendDidChange() {
        if (client == null || !client.isReady()) return;
        ensureOpened();
        int v = version.incrementAndGet();
        client.didChange(uri, textArea.getText(), v);
    }

    public void close() {
        debounceTimer.stop();
        if (client != null) {
            client.didClose(uri);
        }
    }

    public CompletableFuture<List<org.eclipse.lsp4j.CompletionItem>> requestCompletion() {
        if (client == null || !client.isReady()) {
            return CompletableFuture.completedFuture(List.of());
        }
        ensureOpened();
        int caret = textArea.getCaretPosition();
        Position pos = offsetToPosition(caret);
        return client.completion(uri, pos);
    }

    public CompletableFuture<String> requestHover() {
        if (client == null || !client.isReady()) {
            return CompletableFuture.completedFuture(null);
        }
        ensureOpened();
        Position pos = offsetToPosition(textArea.getCaretPosition());
        return client.hover(uri, pos).thenApply(hover -> {
            if (hover == null) return null;
            if (hover.getContents() == null) return null;
            if (hover.getContents().isRight()) {
                return hover.getContents().getRight().getValue();
            } else if (hover.getContents().isLeft()) {
                StringBuilder sb = new StringBuilder();
                for (var ms : hover.getContents().getLeft()) {
                    sb.append(ms.getValue()).append('\n');
                }
                return sb.toString().trim();
            }
            return null;
        });
    }

    private Position offsetToPosition(int offset) {
        try {
            int line = textArea.getLineOfOffset(offset);
            int lineStart = textArea.getLineStartOffset(line);
            int col = offset - lineStart;
            return new Position(line, col);
        } catch (BadLocationException e) {
            return new Position(0, 0);
        }
    }

    private void renderDiagnostics(List<Diagnostic> list) {
        highlighter.removeAllHighlights();
        if (list == null || list.isEmpty()) {
            textArea.setToolTipText(null);
            tooltipTag = null;
            return;
        }
        String text = textArea.getText();
        OffsetMapper mapper = new OffsetMapper(text);
        for (Diagnostic d : list) {
            Range r = d.getRange();
            int start = mapper.toOffset(r.getStart().getLine(), r.getStart().getCharacter(), text.length());
            int end = mapper.toOffset(r.getEnd().getLine(), r.getEnd().getCharacter(), text.length());
            int len = Math.max(1, end - start);
            try {
                Color color = d.getSeverity() != null && d.getSeverity().getValue() <= 1
                        ? new Color(255, 120, 120)
                        : new Color(255, 180, 120);
                highlighter.addHighlight(start, start + len, new DefaultHighlighter.DefaultHighlightPainter(color));
            } catch (BadLocationException ignored) {
            }
        }
    }

    private void showTooltip(Point point) {
        if (point == null) return;
        int pos = textArea.viewToModel(point);
        List<Diagnostic> list = diagnosticsStore.get(uri);
        if (list.isEmpty()) {
            textArea.setToolTipText(null);
            return;
        }
        String text = textArea.getText();
        OffsetMapper mapper = new OffsetMapper(text);
        for (Diagnostic d : list) {
            Range r = d.getRange();
            int start = mapper.toOffset(r.getStart().getLine(), r.getStart().getCharacter(), text.length());
            int end = mapper.toOffset(r.getEnd().getLine(), r.getEnd().getCharacter(), text.length());
            if (pos >= start && pos <= end) {
                String msg = d.getMessage();
                if (d.getSource() != null) {
                    msg += " (" + d.getSource() + ")";
                }
                textArea.setToolTipText(msg);
                return;
            }
        }
        textArea.setToolTipText(null);
    }
}
