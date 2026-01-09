package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ColumnDisplayNameResolverTest {

    @Test
    void usesTableNameWhenDuplicate() {
        ColumnMeta left = new ColumnMeta();
        left.setName("acc_nbr");
        left.setTableName("a");
        ColumnMeta right = new ColumnMeta();
        right.setName("acc_nbr");
        right.setTableName("b");

        List<String> display = ColumnDisplayNameResolver.resolveDisplayNames(
                List.of("acc_nbr", "acc_nbr"),
                List.of(left, right)
        );

        assertEquals(List.of("a.acc_nbr", "b.acc_nbr"), display);
    }

    @Test
    void suffixesWhenNoTableName() {
        List<String> display = ColumnDisplayNameResolver.resolveDisplayNames(
                List.of("acc_nbr", "acc_nbr", "acc_nbr"),
                List.of()
        );

        assertEquals(List.of("acc_nbr", "acc_nbr#2", "acc_nbr#3"), display);
    }
}
