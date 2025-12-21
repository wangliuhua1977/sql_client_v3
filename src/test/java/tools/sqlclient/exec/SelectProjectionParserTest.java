package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SelectProjectionParserTest {

    @Test
    void parsesStarWithAliasAndExpressionAlias() {
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(
                "select a.*, a.acct_id a1 from wlh_acct_item_owe a limit 5;");

        assertNotNull(projection);
        List<SelectProjectionParser.ProjectionItem> items = projection.items();
        assertEquals(2, items.size());
        assertEquals(SelectProjectionParser.ProjectionItem.Type.STAR, items.get(0).type());
        assertEquals("a", items.get(0).tableAlias());
        assertEquals(SelectProjectionParser.ProjectionItem.Type.EXPR, items.get(1).type());
        assertEquals("a.acct_id", items.get(1).expression());
        assertEquals("a1", items.get(1).alias());
    }

    @Test
    void parsesStarWithDuplicatedExpressionWithoutAlias() {
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(
                "select a.*, a.acct_id from wlh_acct_item_owe a limit 5;");

        assertNotNull(projection);
        List<SelectProjectionParser.ProjectionItem> items = projection.items();
        assertEquals(2, items.size());
        assertEquals(SelectProjectionParser.ProjectionItem.Type.STAR, items.get(0).type());
        assertEquals("a", items.get(0).tableAlias());
        assertEquals(SelectProjectionParser.ProjectionItem.Type.EXPR, items.get(1).type());
        assertEquals("a.acct_id", items.get(1).expression());
        assertNull(items.get(1).alias());
    }
}
