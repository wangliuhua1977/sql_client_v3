package tools.sqlclient.ui;

import tools.sqlclient.metadata.MetadataService;
import tools.sqlclient.pg.PgRoutineService;
import tools.sqlclient.pg.RoutineInfo;

import javax.swing.*;
import java.awt.*;

/**
 * 简易对象浏览器，可按表名模糊搜索，展示表/视图/函数/过程与其字段。由菜单触发。
 */
public class ObjectBrowserDialog extends JDialog {
    private final ObjectBrowserPanel panel;

    public interface RoutineActionHandler {
        void openRoutine(Component source, RoutineInfo info, boolean editable);

        void runRoutine(Component source, RoutineInfo info);
    }

    public ObjectBrowserDialog(JFrame owner, MetadataService metadataService,
                               PgRoutineService routineService,
                               RoutineActionHandler routineHandler) {
        super(owner, "对象浏览器", false);
        setSize(420, 600);
        setLocationRelativeTo(owner);
        setLayout(new BorderLayout());
        panel = new ObjectBrowserPanel(metadataService, routineService, new ObjectBrowserPanel.RoutineActionHandler() {
            @Override
            public void openRoutine(Component source, RoutineInfo info, boolean editable) {
                routineHandler.openRoutine(source, info, editable);
            }

            @Override
            public void runRoutine(Component source, RoutineInfo info) {
                routineHandler.runRoutine(source, info);
            }
        });
        add(panel, BorderLayout.CENTER);
    }

    public void reload() {
        panel.reload();
    }
}
