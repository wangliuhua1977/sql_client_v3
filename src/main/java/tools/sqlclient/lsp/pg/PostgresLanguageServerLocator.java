package tools.sqlclient.lsp.pg;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * 负责定位 postgres-language-server.exe 的工具类。
 * 规则：进程所在目录 > jar 目录 > user.dir > PATH。
 */
public final class PostgresLanguageServerLocator {
    private static final String EXECUTABLE = "postgres-language-server.exe";

    private PostgresLanguageServerLocator() {
    }

    public static Optional<Path> locateExecutable() {
        Optional<Path> fromProcess = locateFromCurrentProcess();
        if (fromProcess.isPresent()) {
            return fromProcess;
        }
        Optional<Path> fromJar = locateFromCodeSource();
        if (fromJar.isPresent()) {
            return fromJar;
        }
        Optional<Path> fromUserDir = locateFromUserDir();
        if (fromUserDir.isPresent()) {
            return fromUserDir;
        }
        return locateFromPathEnv();
    }

    private static Optional<Path> locateFromCurrentProcess() {
        try {
            return ProcessHandle.current()
                    .info()
                    .command()
                    .map(Paths::get)
                    .map(Path::getParent)
                    .map(parent -> parent.resolve(EXECUTABLE))
                    .filter(Files::isRegularFile);
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private static Optional<Path> locateFromCodeSource() {
        try {
            var url = PostgresLanguageServerLocator.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation();
            if (url == null) {
                return Optional.empty();
            }
            Path path = Paths.get(url.toURI()).toAbsolutePath();
            Path dir = Files.isDirectory(path) ? path : path.getParent();
            if (dir == null) {
                return Optional.empty();
            }
            Path candidate = dir.resolve(EXECUTABLE);
            return Files.isRegularFile(candidate) ? Optional.of(candidate) : Optional.empty();
        } catch (Exception ignored) {
            return Optional.empty();
        }
    }

    private static Optional<Path> locateFromUserDir() {
        String userDir = System.getProperty("user.dir");
        if (userDir == null || userDir.isBlank()) {
            return Optional.empty();
        }
        Path candidate = Paths.get(userDir).resolve(EXECUTABLE);
        return Files.isRegularFile(candidate) ? Optional.of(candidate) : Optional.empty();
    }

    private static Optional<Path> locateFromPathEnv() {
        String pathEnv = System.getenv("PATH");
        if (pathEnv == null || pathEnv.isBlank()) {
            return Optional.empty();
        }
        for (String part : pathEnv.split(File.pathSeparator)) {
            if (part == null || part.isBlank()) continue;
            Path candidate = Paths.get(part.trim()).resolve(EXECUTABLE);
            if (Files.isRegularFile(candidate)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }
}
