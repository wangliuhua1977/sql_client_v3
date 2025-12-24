package tools.sqlclient.exec;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlTopLevelClassifierTest {

    @Test
    void ctasIsNonQuery() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.NON_QUERY,
                SqlTopLevelClassifier.classify("create table tmp as select * from t"));
    }

    @Test
    void insertSelectIsNonQuery() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.NON_QUERY,
                SqlTopLevelClassifier.classify("insert into t select * from src"));
    }

    @Test
    void updateWithSelectClauseIsNonQuery() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.NON_QUERY,
                SqlTopLevelClassifier.classify("update t set a = (select 1)"));
    }

    @Test
    void withSelectReturnsResultSet() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.RESULT_SET,
                SqlTopLevelClassifier.classify("with c as (select 1) select * from c"));
    }

    @Test
    void withInsertIsNonQuery() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.NON_QUERY,
                SqlTopLevelClassifier.classify("with c as (select 1) insert into t select * from c"));
    }

    @Test
    void explainSelectIsResultSet() {
        assertEquals(SqlTopLevelClassifier.TopLevelType.RESULT_SET,
                SqlTopLevelClassifier.classify("explain (analyze, buffers) select * from t"));
    }
}
