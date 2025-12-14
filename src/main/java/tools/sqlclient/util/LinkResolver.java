package tools.sqlclient.util;

import tools.sqlclient.db.NoteRepository;
import tools.sqlclient.model.Note;
import tools.sqlclient.util.OperationLog;

import javax.swing.*;
import javax.swing.text.BadLocationException;
import javax.swing.text.DefaultHighlighter;
import javax.swing.text.Highlighter;
import javax.swing.text.JTextComponent;
import java.awt.*;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Obsidian 风格的双向链接解析与辅助工具。
 */
public class LinkResolver {

    /**
     * 表示一次 [[...]] 链接的出现位置与拆解结果。
     */
    public static class LinkRef {
        public final int startOffset; // 含 [[ 的起始位置
        public final int endOffset;   // 含 ]] 的结束位置（闭区间）
        public final String rawText;  // [[...]] 原文
        public final String targetTitle;
        public final String displayText;

        public LinkRef(int startOffset, int endOffset, String rawText, String targetTitle, String displayText) {
            this.startOffset = startOffset;
            this.endOffset = endOffset;
            this.rawText = rawText;
            this.targetTitle = targetTitle;
            this.displayText = displayText;
        }
    }

    private static final Highlighter.HighlightPainter LINK_PAINTER =
            new DefaultHighlighter.DefaultHighlightPainter(new Color(198, 223, 255));

    /**
     * 安装鼠标监听，实现点击/Ctrl+点击跳转，并为 [[...]] 添加简单的高亮效果。
     *
     * @param area         需要安装的文本组件（RSyntaxTextArea 兼容 JTextComponent 接口）
     * @param clickHandler 链接点击回调，传入解析出的 LinkRef；可为 null 仅高亮不跳转
     */
    public static void install(JTextComponent area, Consumer<LinkRef> clickHandler) {
        Objects.requireNonNull(area, "area");
        LinkHighlighter highlighter = new LinkHighlighter(area);
        area.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                if (!SwingUtilities.isLeftMouseButton(e)) return;
                if (!(e.isControlDown() || e.getClickCount() >= 2)) return;
                int pos = area.viewToModel(e.getPoint());
                LinkRef ref = findLinkAt(area.getText(), pos);
                if (ref != null && clickHandler != null) {
                    clickHandler.accept(ref);
                }
            }
        });
        area.addMouseMotionListener(new java.awt.event.MouseMotionAdapter() {
            @Override
            public void mouseMoved(MouseEvent e) {
                int pos = area.viewToModel(e.getPoint());
                LinkRef ref = findLinkAt(area.getText(), pos);
                if (ref != null) {
                    area.setCursor(Cursor.getPredefinedCursor(Cursor.HAND_CURSOR));
                } else {
                    area.setCursor(Cursor.getDefaultCursor());
                }
            }
        });
        area.getDocument().addDocumentListener(highlighter);
        SwingUtilities.invokeLater(highlighter::refresh);
    }

    /**
     * 兼容旧签名，保留点击弹窗提示。
     */
    public static void install(JTextArea area) {
        install(area, ref -> JOptionPane.showMessageDialog(area,
                "跳转到: " + ref.targetTitle + "\n(请在主界面实现打开笔记的逻辑)",
                "双向链接", JOptionPane.INFORMATION_MESSAGE));
    }

    /**
     * 解析文本中的所有 [[...]] 链接，遵循“同一行内、无嵌套”规则。
     */
    public static List<LinkRef> parseLinks(String text) {
        List<LinkRef> refs = new ArrayList<>();
        if (text == null || text.isEmpty()) {
            return refs;
        }
        int idx = 0;
        while (idx < text.length()) {
            int start = text.indexOf("[[", idx);
            if (start < 0) break;
            int lineEnd = text.indexOf('\n', start);
            if (lineEnd < 0) lineEnd = text.length();
            int closing = text.indexOf("]]", start + 2);
            int nested = text.indexOf("[[", start + 2);
            if (nested >= 0 && (closing < 0 || nested < closing)) {
                idx = nested;
                continue; // 出现嵌套，前一个作废
            }
            if (closing < 0 || closing >= lineEnd) {
                idx = lineEnd; // 同行未找到闭合，跳到下一行
                continue;
            }
            String raw = text.substring(start, closing + 2);
            String inner = text.substring(start + 2, closing);
            String target = inner;
            String display = inner;
            int pipe = inner.indexOf('|');
            if (pipe >= 0) {
                target = inner.substring(0, pipe);
                display = inner.substring(pipe + 1);
            }
            target = target.trim();
            display = display.trim();
            if (!target.isEmpty()) {
                refs.add(new LinkRef(start, closing + 1, raw, target, display.isEmpty() ? target : display));
            }
            idx = closing + 2;
        }
        return refs;
    }

    /**
     * 删除文本中的所有 [[...]] 标签，用于 SQL 执行时剥离。
     */
    public static String stripLinkTags(String text) {
        if (text == null || text.isEmpty()) return "";
        List<LinkRef> refs = parseLinks(text);
        if (refs.isEmpty()) return text;
        StringBuilder sb = new StringBuilder(text);
        for (int i = refs.size() - 1; i >= 0; i--) {
            LinkRef ref = refs.get(i);
            int endExclusive = Math.min(sb.length(), ref.endOffset + 1);
            sb.delete(ref.startOffset, endExclusive);
        }
        return sb.toString();
    }

    /**
     * 解析并持久化链接关系：解析文本 -> 按标题查找/创建目标笔记 -> 更新 note_links。
     */
    public static Set<Long> resolveAndPersistLinks(NoteRepository noteRepo, Note currentNote, String text) {
        Objects.requireNonNull(noteRepo, "noteRepo");
        Objects.requireNonNull(currentNote, "currentNote");
        List<LinkRef> refs = parseLinks(text);
        Set<Long> targets = new HashSet<>();
        for (LinkRef ref : refs) {
            try {
                Note target = noteRepo.findOrCreateByTitle(ref.targetTitle);
                targets.add(target.getId());
            } catch (Exception ex) {
                OperationLog.log("解析链接失败: " + ref.targetTitle + " | " + ex.getMessage());
            }
        }
        try {
            noteRepo.updateNoteLinks(currentNote.getId(), targets);
        } catch (Exception ex) {
            OperationLog.log("保存笔记关联失败: " + ex.getMessage());
        }
        return targets;
    }

    private static LinkRef findLinkAt(String text, int pos) {
        if (text == null || pos < 0) return null;
        for (LinkRef ref : parseLinks(text)) {
            if (pos >= ref.startOffset && pos <= ref.endOffset) {
                return ref;
            }
        }
        return null;
    }

    /**
     * 简单的文档监听器：在文本变更时重新高亮所有链接。
     */
    private static class LinkHighlighter implements javax.swing.event.DocumentListener {
        private final JTextComponent area;
        private final java.util.List<Object> tags = new ArrayList<>();

        LinkHighlighter(JTextComponent area) {
            this.area = area;
        }

        @Override
        public void insertUpdate(javax.swing.event.DocumentEvent e) {
            refreshLater();
        }

        @Override
        public void removeUpdate(javax.swing.event.DocumentEvent e) {
            refreshLater();
        }

        @Override
        public void changedUpdate(javax.swing.event.DocumentEvent e) {
            refreshLater();
        }

        private void refreshLater() {
            SwingUtilities.invokeLater(this::refresh);
        }

        private void clearHighlights() {
            Highlighter highlighter = area.getHighlighter();
            for (Object tag : tags) {
                highlighter.removeHighlight(tag);
            }
            tags.clear();
        }

        void refresh() {
            clearHighlights();
            String text = area.getText();
            for (LinkRef ref : parseLinks(text)) {
                try {
                    Object tag = area.getHighlighter().addHighlight(ref.startOffset, ref.endOffset + 1, LINK_PAINTER);
                    tags.add(tag);
                } catch (BadLocationException ignored) {
                }
            }
        }
    }
}
