package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class RowValueResolverTest {

    @Test
    void resolvesSingleColumnFromOnlyValue() {
        List<ColumnDef> defs = List.of(new ColumnDef("c1", "sum(amount)/100", "sum_amount_100"));
        Map<String, String> row = new LinkedHashMap<>();
        row.put("?column?", "272681.8");

        Object value = RowValueResolver.resolveValue(defs, row, 0);

        assertEquals("272681.8", value);
    }

    @Test
    void resolvesCaseInsensitiveKey() {
        List<ColumnDef> defs = List.of(new ColumnDef("c1", "sum(amount)/100", "sum"));
        Map<String, String> row = new LinkedHashMap<>();
        row.put("SUM", "99.5");

        Object value = RowValueResolver.resolveValue(defs, row, 0);

        assertEquals("99.5", value);
    }

    @Test
    void resolvesByOrderForLinkedHashMap() {
        List<ColumnDef> defs = List.of(
                new ColumnDef("c1", "col_a", "col_a"),
                new ColumnDef("c2", "col_b", "col_b")
        );
        Map<String, String> row = new LinkedHashMap<>();
        row.put("x", "v1");
        row.put("y", "v2");

        assertEquals("v1", RowValueResolver.resolveValue(defs, row, 0));
        assertEquals("v2", RowValueResolver.resolveValue(defs, row, 1));
    }

    @Test
    void resolvesFromArrayRow() {
        List<String> columns = List.of("a", "b");
        List<String> row = List.of("v1", "v2");

        Object value = RowValueResolver.resolveValueByName(columns, row, 1);

        assertEquals("v2", value);
    }

    @Test
    void handlesEmptyRowsSafely() {
        List<String> columns = List.of("only_col");
        Map<String, String> row = Map.of();

        assertNull(RowValueResolver.resolveValueByName(columns, row, 0));
    }
}
