package tools.sqlclient.ui;

import org.junit.jupiter.api.Test;
import tools.sqlclient.exec.ColumnDef;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColumnarTableModelTest {

    @Test
    void rendersByColumnIndex() {
        ColumnarTableModel model = new ColumnarTableModel();
        List<ColumnDef> defs = List.of(
                new ColumnDef("acc_nbr#1", "acc_nbr", "acc_nbr"),
                new ColumnDef("acc_nbr#2", "acc_nbr#2", "acc_nbr"),
                new ColumnDef("count_all#3", "count_all", "count_all")
        );
        List<List<String>> rows = List.of(
                List.of("a1", "a2", "c1"),
                List.of("b1", "b2", "c2")
        );
        model.setData(defs, rows);

        assertEquals("a1", model.getValueAt(0, 0));
        assertEquals("a2", model.getValueAt(0, 1));
        assertEquals("c1", model.getValueAt(0, 2));
        assertEquals("b2", model.getValueAt(1, 1));
    }
}
