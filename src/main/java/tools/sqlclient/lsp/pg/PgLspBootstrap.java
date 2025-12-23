package tools.sqlclient.lsp.pg;

import org.eclipse.lsp4j.services.LanguageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.util.OperationLog;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

/**
 * 负责创建配置/日志目录并启动 postgres-language-server.exe。
 */
public class PgLspBootstrap {
    private static final Logger log = LoggerFactory.getLogger(PgLspBootstrap.class);
    private Process process;
    private Path logFile;

    public synchronized PgLspClient startIfPossible() {
        if (process != null && process.isAlive()) {
            return null;
        }
        Optional<Path> exe = PostgresLanguageServerLocator.locateExecutable();
        if (exe.isEmpty()) {
            OperationLog.log("未找到 postgres-language-server.exe，语义功能已禁用");
            javax.swing.SwingUtilities.invokeLater(() -> javax.swing.JOptionPane.showMessageDialog(
                    null,
                    "未找到 postgres-language-server.exe，语法补全/诊断已禁用",
                    "提示",
                    javax.swing.JOptionPane.WARNING_MESSAGE
            ));
            return null;
        }
        try {
            Path configDir = prepareConfigDir();
            Path logDir = prepareLogDir();
            logFile = logDir.resolve("pg-lsp.log");
            ProcessBuilder pb = new ProcessBuilder(
                    exe.get().toAbsolutePath().toString(),
                    "--skip-db",
                    "lsp-proxy",
                    "--config-path",
                    configDir.toString(),
                    "--log-path",
                    logDir.toString(),
                    "--log-prefix-name",
                    "sql_client_v3"
            );
            pb.redirectErrorStream(true);
            process = pb.start();
            pumpLogs(process.getInputStream(), logFile);
            OperationLog.log("postgres-language-server 已启动: " + exe.get());
            return new PgLspClient(process.getOutputStream(), process.getInputStream(), this::shutdownProcess);
        } catch (Exception e) {
            OperationLog.log("启动 postgres-language-server 失败: " + e.getMessage());
            log.warn("启动失败", e);
            return null;
        }
    }

    private void pumpLogs(InputStream in, Path logFile) {
        Thread t = new Thread(() -> {
            try (InputStream src = in; OutputStream out = new FileOutputStream(logFile.toFile(), true)) {
                byte[] buf = new byte[4096];
                int n;
                while ((n = src.read(buf)) >= 0) {
                    out.write(buf, 0, n);
                }
            } catch (Exception e) {
                log.warn("写入 LSP 日志失败: {}", e.getMessage());
            }
        }, "pg-lsp-log-pump");
        t.setDaemon(true);
        t.start();
    }

    private Path prepareConfigDir() throws Exception {
        String appData = System.getenv("APPDATA");
        Path base = (appData != null && !appData.isBlank())
                ? Path.of(appData)
                : Path.of(System.getProperty("user.home", "."));
        Path target = base.resolve("sql_client_v3").resolve("pgls");
        Files.createDirectories(target);
        return target;
    }

    private Path prepareLogDir() throws Exception {
        Path dir = Path.of("logs");
        Files.createDirectories(dir);
        return dir;
    }

    private void shutdownProcess() {
        Process p = this.process;
        if (p == null) {
            return;
        }
        try {
            if (!p.isAlive()) {
                return;
            }
            p.waitFor(2, java.util.concurrent.TimeUnit.SECONDS);
            if (p.isAlive()) {
                p.destroyForcibly();
            }
        } catch (Exception e) {
            log.warn("关闭 postgres-language-server 进程失败: {}", e.getMessage());
        }
    }
}
