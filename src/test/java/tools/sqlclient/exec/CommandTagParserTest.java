package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class CommandTagParserTest {

    @Test
    void parsesInsertTag() {
        assertEquals(3, CommandTagParser.parseAffectedRows("INSERT 3").orElse(-1));
    }

    @Test
    void parsesSelectTag() {
        assertEquals(10, CommandTagParser.parseAffectedRows("select 10").orElse(-1));
    }

    @Test
    void ignoresInvalidTag() {
        assertFalse(CommandTagParser.parseAffectedRows("CREATE TABLE").isPresent());
        assertFalse(CommandTagParser.parseAffectedRows(null).isPresent());
    }
}
