package tools.sqlclient.ui;

import org.junit.jupiter.api.Test;
import tools.sqlclient.exec.ColumnDef;

import javax.swing.event.TableModelEvent;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColumnarTableModelTest {

    @Test
    void setValueAtUpdatesDataAndFiresEvent() {
        ColumnarTableModel model = new ColumnarTableModel();
        model.setData(List.of(new ColumnDef("col1", "col1", "col1")), List.of(List.of("old")));

        AtomicInteger updates = new AtomicInteger();
        model.addTableModelListener(event -> {
            if (event.getType() == TableModelEvent.UPDATE) {
                updates.incrementAndGet();
            }
        });

        model.setValueAt("new", 0, 0);

        assertEquals("new", model.getValueAt(0, 0));
        assertEquals(1, updates.get());
    }

    @Test
    void editorDialogServiceApplyWritesValue() {
        ColumnarTableModel model = new ColumnarTableModel();
        model.setData(List.of(new ColumnDef("col1", "col1", "col1")), List.of(List.of("before")));

        EditorDialogService service = new EditorDialogService();
        service.apply(model, 0, 0, "after");

        assertEquals("after", model.getValueAt(0, 0));
    }
}
