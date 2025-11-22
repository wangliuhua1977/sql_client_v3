package tools.sqlclient.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 全局线程池：网络、自动保存、元数据维护。可按需扩展。
 */
public class ThreadPools {
    public static final ExecutorService NETWORK_POOL = Executors.newFixedThreadPool(6, r -> new Thread(r, "network"));
    public static final ExecutorService AUTOSAVE_POOL = Executors.newFixedThreadPool(2, r -> new Thread(r, "autosave"));
    public static final ExecutorService METADATA_POOL = Executors.newFixedThreadPool(4, r -> new Thread(r, "metadata"));
}
