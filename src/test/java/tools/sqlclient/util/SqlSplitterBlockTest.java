package tools.sqlclient.util;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SqlSplitterBlockTest {

    @Test
    void splitBlocksRespectsEmptyLines_case1() {
        String text = """
                create table tmp_wlh_vvvv1 as
                select * from mt_icb_offer_inst_attr

                select * from tmp_wlh_jk3;
                """;

        SqlSplitter.SqlBlock secondBlock = SqlSplitter.findBlockAtCaret(text, text.indexOf("tmp_wlh_jk3"));
        assertNotNull(secondBlock);
        List<String> statementsSecond = SqlSplitter.splitBlockToStatements(secondBlock.text());
        assertEquals(List.of("select * from tmp_wlh_jk3"), statementsSecond);

        SqlSplitter.SqlBlock firstBlock = SqlSplitter.findBlockAtCaret(text, text.indexOf("tmp_wlh_vvvv1"));
        assertNotNull(firstBlock);
        List<String> statementsFirst = SqlSplitter.splitBlockToStatements(firstBlock.text());
        assertEquals(1, statementsFirst.size());
        assertEquals("create table tmp_wlh_vvvv1 as\nselect * from mt_icb_offer_inst_attr", statementsFirst.get(0));
    }

    @Test
    void multiStatementsInSameBlockExecuteTogether_case2() {
        String text = "select 1; select 2;";
        SqlSplitter.SqlBlock block = SqlSplitter.findBlockAtCaret(text, text.indexOf("select 2"));
        assertNotNull(block);
        List<String> statements = SqlSplitter.splitBlockToStatements(block.text());
        assertEquals(List.of("select 1", "select 2"), statements);
    }

    @Test
    void semicolonInsideCommentOrStringIgnored_case3() {
        String text = """
                select ';' as s; -- comment ;
                select 2;
                """;
        SqlSplitter.SqlBlock block = SqlSplitter.findBlockAtCaret(text, text.indexOf("comment"));
        assertNotNull(block);
        List<String> statements = SqlSplitter.splitBlockToStatements(block.text());
        assertEquals(List.of("select ';' as s", "select 2"), statements);
    }

    @Test
    void dollarQuoteSemicolonIgnored_case4() {
        String text = """
                select $$a;b$$ as t;
                select 2;
                """;
        SqlSplitter.SqlBlock block = SqlSplitter.findBlockAtCaret(text, text.indexOf("a;b"));
        assertNotNull(block);
        List<String> statements = SqlSplitter.splitBlockToStatements(block.text());
        assertEquals(List.of("select $$a;b$$ as t", "select 2"), statements);
    }

    @Test
    void caretOnEmptyLineChoosesNextBlock_case5() {
        String text = """
                select 1;

                select 2;
                """;
        int caret = text.indexOf("\n\n") + 1; // position on the blank line
        SqlSplitter.SqlBlock block = SqlSplitter.findBlockAtCaret(text, caret);
        assertNotNull(block);
        List<String> statements = SqlSplitter.splitBlockToStatements(block.text());
        assertEquals(List.of("select 2"), statements);
    }
}

