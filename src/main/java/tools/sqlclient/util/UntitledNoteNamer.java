package tools.sqlclient.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class UntitledNoteNamer {
    private static final String BASE_TITLE = "未命名";
    private static final AtomicInteger COUNTER = new AtomicInteger(1);

    private UntitledNoteNamer() {
    }

    public static String nextTitle() {
        return BASE_TITLE + " " + COUNTER.getAndIncrement();
    }

    public static String nextTitle(Predicate<String> exists) {
        String title;
        do {
            title = nextTitle();
        } while (exists != null && exists.test(title));
        return title;
    }

    static void resetForTest(int start) {
        COUNTER.set(start);
    }
}
