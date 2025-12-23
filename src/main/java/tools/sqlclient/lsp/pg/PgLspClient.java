package tools.sqlclient.lsp.pg;

import org.eclipse.lsp4j.CompletionItem;
import org.eclipse.lsp4j.CompletionList;
import org.eclipse.lsp4j.Diagnostic;
import org.eclipse.lsp4j.DidChangeTextDocumentParams;
import org.eclipse.lsp4j.DidCloseTextDocumentParams;
import org.eclipse.lsp4j.DidOpenTextDocumentParams;
import org.eclipse.lsp4j.DidSaveTextDocumentParams;
import org.eclipse.lsp4j.Hover;
import org.eclipse.lsp4j.InitializeParams;
import org.eclipse.lsp4j.InitializeResult;
import org.eclipse.lsp4j.MarkupContent;
import org.eclipse.lsp4j.Position;
import org.eclipse.lsp4j.ServerCapabilities;
import org.eclipse.lsp4j.TextDocumentContentChangeEvent;
import org.eclipse.lsp4j.TextDocumentIdentifier;
import org.eclipse.lsp4j.TextDocumentItem;
import org.eclipse.lsp4j.TextDocumentSyncKind;
import org.eclipse.lsp4j.TextDocumentSyncOptions;
import org.eclipse.lsp4j.jsonrpc.Launcher;
import org.eclipse.lsp4j.services.LanguageClient;
import org.eclipse.lsp4j.services.LanguageServer;
import org.eclipse.lsp4j.services.TextDocumentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.util.OperationLog;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * LSP 客户端封装。
 */
public class PgLspClient implements LanguageClient {
    private static final Logger log = LoggerFactory.getLogger(PgLspClient.class);
    private final ExecutorService executor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "pg-lsp-client");
        t.setDaemon(true);
        return t;
    });
    private final OutputStream serverStdin;
    private final InputStream serverStdout;
    private LanguageServer server;
    private TextDocumentService textService;
    private Consumer<PublishDiagnosticsArgs> diagnosticsHandler;
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final Runnable onShutdown;

    public PgLspClient(OutputStream serverStdin, InputStream serverStdout, Runnable onShutdown) {
        this.serverStdin = serverStdin;
        this.serverStdout = serverStdout;
        this.onShutdown = onShutdown;
    }

    public void setDiagnosticsHandler(Consumer<PublishDiagnosticsArgs> diagnosticsHandler) {
        this.diagnosticsHandler = diagnosticsHandler;
    }

    public CompletableFuture<Boolean> initialize() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                Launcher<LanguageServer> launcher = Launcher.createLauncher(this, LanguageServer.class,
                        serverStdout, serverStdin, executor, m -> log.debug("pg-lsp msg: {}", m));
                launcher.startListening();
                this.server = launcher.getRemoteProxy();
                this.textService = server.getTextDocumentService();

                InitializeParams params = new InitializeParams();
                params.setProcessId((int) ProcessHandle.current().pid());
                params.setCapabilities(new org.eclipse.lsp4j.ClientCapabilities());
                InitializeResult result = server.initialize(params).get();
                ServerCapabilities caps = result.getCapabilities();
                OperationLog.log("LSP 初始化完成，capabilities=" + caps);
                initialized.set(true);
                return true;
            } catch (Exception e) {
                log.warn("初始化 LSP 失败", e);
                OperationLog.log("初始化语义服务失败: " + e.getMessage());
                return false;
            }
        }, executor);
    }

    public boolean isReady() {
        return initialized.get() && server != null && textService != null;
    }

    public void didOpen(String uri, String languageId, String text, int version) {
        if (!isReady()) return;
        TextDocumentItem item = new TextDocumentItem(uri, languageId, version, text);
        DidOpenTextDocumentParams params = new DidOpenTextDocumentParams(item);
        textService.didOpen(params);
    }

    public void didChange(String uri, String text, int version) {
        if (!isReady()) return;
        TextDocumentContentChangeEvent change = new TextDocumentContentChangeEvent();
        change.setText(text);
        DidChangeTextDocumentParams params = new DidChangeTextDocumentParams();
        params.setTextDocument(new org.eclipse.lsp4j.VersionedTextDocumentIdentifier(uri, version));
        params.setContentChanges(List.of(change));
        textService.didChange(params);
    }

    public void didClose(String uri) {
        if (!isReady()) return;
        TextDocumentIdentifier id = new TextDocumentIdentifier(uri);
        textService.didClose(new DidCloseTextDocumentParams(id));
    }

    public CompletableFuture<List<CompletionItem>> completion(String uri, Position pos) {
        if (!isReady()) {
            return CompletableFuture.completedFuture(Collections.emptyList());
        }
        return textService.completion(new org.eclipse.lsp4j.CompletionParams(new TextDocumentIdentifier(uri), pos))
                .thenApply(res -> {
                    if (res == null) return Collections.emptyList();
                    if (res.isLeft()) {
                        CompletionList list = res.getLeft();
                        return list.getItems() == null ? Collections.emptyList() : list.getItems();
                    }
                    return res.getRight();
                });
    }

    public CompletableFuture<Hover> hover(String uri, Position pos) {
        if (!isReady()) {
            return CompletableFuture.completedFuture(null);
        }
        return textService.hover(new org.eclipse.lsp4j.HoverParams(new TextDocumentIdentifier(uri), pos));
    }

    public void shutdown() {
        if (server == null) {
            return;
        }
        try {
            server.shutdown().get(2, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception ignored) {
        }
        try {
            server.exit();
        } catch (Exception ignored) {
        }
        executor.shutdownNow();
        if (onShutdown != null) {
            onShutdown.run();
        }
    }

    @Override
    public void publishDiagnostics(org.eclipse.lsp4j.PublishDiagnosticsParams diagnostics) {
        Consumer<PublishDiagnosticsArgs> handler = diagnosticsHandler;
        if (handler == null) {
            return;
        }
        handler.accept(new PublishDiagnosticsArgs(diagnostics.getUri(), diagnostics.getDiagnostics()));
    }

    public record PublishDiagnosticsArgs(String uri, List<Diagnostic> diagnostics) { }
}
