package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;
import tools.sqlclient.ui.ColumnarTableModel;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ColumnLayoutBuilderTest {

    private final List<String> tableColumns = List.of(
            "billing_cycle_id", "acct_id", "amount", "payment_mode", "acct_item_status_cd", "amount_cs", "prod_inst_id", "owe_age"
    );

    @Test
    void starExpansionKeepsOrderAndAppendsAlias() {
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(
                "select a.*, a.acct_id a1 from wlh_acct_item_owe a limit 5;");
        ColumnLayoutBuilder builder = new ColumnLayoutBuilder(Map.of("a", tableColumns));

        ColumnLayoutBuilder.ColumnLayout layout = builder.build(
                projection,
                List.of(),
                List.of(Map.of("acct_id", "100", "billing_cycle_id", "202311", "owe_age", "12"))
        );

        assertEquals(tableColumns.size() + 1, layout.columnDefs().size());
        assertEquals(tableColumns, layout.displayColumns().subList(0, tableColumns.size()));
        assertEquals("a1", layout.displayColumns().get(layout.displayColumns().size() - 1));
    }

    @Test
    void duplicateExpressionAfterStarAppearsAtTail() {
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(
                "select a.*, a.acct_id from wlh_acct_item_owe a limit 5;");
        ColumnLayoutBuilder builder = new ColumnLayoutBuilder(Map.of("a", tableColumns));

        ColumnLayoutBuilder.ColumnLayout layout = builder.build(
                projection,
                List.of(),
                List.of(Map.of("acct_id", "200", "billing_cycle_id", "202311", "owe_age", "6"))
        );

        assertEquals(tableColumns.size() + 1, layout.columnDefs().size());
        assertEquals("acct_id", layout.displayColumns().get(layout.displayColumns().size() - 1));
    }

    @Test
    void missingKeysAreExposedAsNullCells() {
        SelectProjectionParser.Projection projection = SelectProjectionParser.parse(
                "select a.* from wlh_acct_item_owe a limit 1;");
        ColumnLayoutBuilder builder = new ColumnLayoutBuilder(Map.of("a", List.of("billing_cycle_id", "acct_id", "amount")));
        Map<String, String> row = new LinkedHashMap<>();
        row.put("billing_cycle_id", "202311");
        row.put("acct_id", "300");

        ColumnLayoutBuilder.ColumnLayout layout = builder.build(projection, List.of(), List.of(row));
        ColumnarTableModel model = new ColumnarTableModel();
        model.setData(layout.columnDefs(), layout.rows());

        assertEquals(3, model.getColumnCount());
        assertNull(model.getValueAt(0, 2));
    }
}
