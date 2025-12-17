package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class ColumnOrderDeciderTest {

    @Test
    void keepsOriginalOrderForCastExpression() {
        SqlExecResult original = new SqlExecResult(
                "select acct_id::varchar from wlh_acct_item_owe a limit 2",
                List.of("acct_id::varchar"),
                List.of(
                        List.of("1001"),
                        List.of("1002")
                ),
                2
        );

        ColumnOrderDecider decider = new ColumnOrderDecider(null);
        SqlExecResult reordered = decider.reorder(original, null);

        assertEquals(original.getColumns(), reordered.getColumns(), "必须优先使用后端返回的列名与顺序");
        assertFalse(reordered.getRows().isEmpty(), "行数据不应被抹空");
        assertEquals(original.getRows().get(0).get(0), reordered.getRows().get(0).get(0));
        assertEquals(reordered.getColumns().size(), reordered.getRows().get(0).size());
    }

    @Test
    void keepsDuplicateColumnsAndAlignmentWithStarMix() {
        List<String> columns = List.of("lx_flag", "acct_id", "acct_id");
        List<List<String>> rows = List.of(
                List.of("Y", "100", "100"),
                List.of("N", "200", "200")
        );
        SqlExecResult original = new SqlExecResult(
                "select a.lx_flag, * from wlh_acct_item_owe a limit 2",
                columns,
                rows,
                2
        );

        ColumnOrderDecider decider = new ColumnOrderDecider(null);
        SqlExecResult reordered = decider.reorder(original, null);

        assertEquals(columns, reordered.getColumns(), "混排列必须保持服务端顺序与重复列");
        assertEquals(rows, reordered.getRows(), "列顺序未变时行数据应原样返回");
        assertEquals(reordered.getColumns().size(), reordered.getRows().get(0).size());
    }
}
