package tools.sqlclient.lsp.pg;

import org.eclipse.lsp4j.CompletionItem;
import tools.sqlclient.metadata.MetadataService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 合并本地元数据与 LSP completion。
 */
public class CompletionMerger {
    public record CompletionEntry(String label, String insertText, String source) { }

    public static List<CompletionEntry> merge(List<MetadataService.SuggestionItem> metadata,
                                              List<CompletionItem> lspItems) {
        Map<String, CompletionEntry> merged = new LinkedHashMap<>();
        if (metadata != null) {
            for (MetadataService.SuggestionItem item : metadata) {
                String key = item.name().toLowerCase() + "|" + item.type();
                merged.put(key, new CompletionEntry(item.name(), item.name(), "Metadata"));
            }
        }
        if (lspItems != null) {
            for (CompletionItem item : lspItems) {
                String label = item.getLabel() != null ? item.getLabel() : "";
                String kind = item.getKind() != null ? item.getKind().name() : "lsp";
                String key = label.toLowerCase() + "|" + kind;
                if (merged.containsKey(key)) continue;
                String insert = item.getInsertText();
                if (item.getTextEdit() != null) {
                    insert = item.getTextEdit().getLeft() != null
                            ? item.getTextEdit().getLeft().getNewText()
                            : item.getTextEdit().getRight() != null
                            ? item.getTextEdit().getRight().getNewText()
                            : insert;
                }
                merged.put(key, new CompletionEntry(label, insert != null ? insert : label, "SQL"));
            }
        }
        return new ArrayList<>(merged.values());
    }
}
