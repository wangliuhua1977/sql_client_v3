package tools.sqlclient.db;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.sqlclient.util.OperationLog;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 统一构造本地 SQLite 路径，确保落在用户目录下并兼容旧版位置。
 */
public final class LocalDatabasePathProvider {
    private static final Logger log = LoggerFactory.getLogger(LocalDatabasePathProvider.class);
    private static final String BASE_DIR_NAME = ".Sql_client_v3";
    private static final String DB_FILE_NAME = "metadata.db";

    private LocalDatabasePathProvider() {
    }

    public static Path resolveMetadataDbPath() {
        Path baseDir = Paths.get(System.getProperty("user.home"), BASE_DIR_NAME);
        Path target = baseDir.resolve(DB_FILE_NAME);
        ensureBaseDir(baseDir);
        migrateIfNeeded(target);
        return target;
    }

    private static void ensureBaseDir(Path baseDir) {
        try {
            Files.createDirectories(baseDir);
        } catch (Exception e) {
            throw new RuntimeException("创建本地数据库目录失败: " + baseDir, e);
        }
    }

    private static void migrateIfNeeded(Path target) {
        Path oldPath = Paths.get("metadata.db").toAbsolutePath().normalize();
        if (Files.notExists(target) && Files.exists(oldPath) && !oldPath.equals(target)) {
            try {
                Files.copy(oldPath, target);
                log.info("已迁移本地数据库到用户目录: {}", target);
                OperationLog.log("已将本地数据库迁移到: " + target);
            } catch (Exception ex) {
                log.warn("迁移旧数据库文件失败: {}", oldPath, ex);
                OperationLog.log("迁移本地数据库失败，请手动检查: " + ex.getMessage());
            }
        } else if (Files.exists(target) && Files.exists(oldPath) && !oldPath.equals(target)) {
            log.info("检测到遗留数据库文件: {}", oldPath);
        }
    }
}

