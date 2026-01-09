package tools.sqlclient.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UntitledNoteNamerTest {
    @Test
    void nextTitleIsSequential() {
        UntitledNoteNamer.resetForTest(1);
        assertEquals("未命名 1", UntitledNoteNamer.nextTitle());
        assertEquals("未命名 2", UntitledNoteNamer.nextTitle());
        assertEquals("未命名 3", UntitledNoteNamer.nextTitle());
    }

    @Test
    void nextTitleSkipsExistingNames() {
        UntitledNoteNamer.resetForTest(1);
        String title = UntitledNoteNamer.nextTitle(t -> t.equals("未命名 1") || t.equals("未命名 2"));
        assertEquals("未命名 3", title);
    }
}
