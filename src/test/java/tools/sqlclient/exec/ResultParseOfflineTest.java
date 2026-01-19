package tools.sqlclient.exec;

import com.google.gson.Gson;
import java.util.List;

public class ResultParseOfflineTest {
    public static void main(String[] args) {
        String json = """
                {
                  "success": true,
                  "status": "SUCCEEDED",
                  "hasResultSet": true,
                  "hasNext": false,
                  "returnedRowCount": 5,
                  "resultRows": [
                    {"billing_cycle_id":202311,"acct_id":450925012,"amount":10000,"payment_mode":2,"acct_item_status_cd":1,"amount_cs":10000,"prod_inst_id":102015000334,"owe_age":24},
                    {"billing_cycle_id":202411,"acct_id":450925012,"amount":208200,"payment_mode":2,"acct_item_status_cd":1,"amount_cs":208200,"prod_inst_id":102015000334,"owe_age":12},
                    {"billing_cycle_id":202311,"acct_id":450925013,"amount":10000,"payment_mode":2,"acct_item_status_cd":1,"amount_cs":10000,"prod_inst_id":102015000335,"owe_age":24},
                    {"billing_cycle_id":202411,"acct_id":450925013,"amount":208200,"payment_mode":2,"acct_item_status_cd":1,"amount_cs":208200,"prod_inst_id":102015000335,"owe_age":12},
                    {"billing_cycle_id":202311,"acct_id":450925014,"amount":10000,"payment_mode":2,"acct_item_status_cd":1,"amount_cs":10000,"prod_inst_id":102015000336,"owe_age":24}
                  ]
                }
                """;
        ResultResponse resp = new Gson().fromJson(json, ResultResponse.class);
        SqlExecutionService service = new SqlExecutionService();
        SqlExecResult result = service.parseResultResponse(resp, "select * from wlh_acct_item_owe limit 5", "job-offline", 1, 5);

        List<String> columns = result.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new IllegalStateException("columns should not be empty");
        }
        if (columns.contains("*")) {
            throw new IllegalStateException("columns must not contain *");
        }
        for (String required : List.of("billing_cycle_id", "acct_id", "amount", "prod_inst_id", "owe_age")) {
            if (!columns.contains(required)) {
                throw new IllegalStateException("missing column: " + required);
            }
        }

        if (result.getRows().size() != 5) {
            throw new IllegalStateException("rows size should be 5 but was " + result.getRows().size());
        }
        String firstCell = result.getRows().get(0).get(columns.indexOf("billing_cycle_id"));
        if (!"202311".equals(firstCell)) {
            throw new IllegalStateException("unexpected first billing_cycle_id: " + firstCell);
        }

        System.out.println("Offline parse test passed: columns=" + columns + ", rows=" + result.getRows().size());
    }
}
