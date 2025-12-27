package tools.sqlclient.app;

import com.formdev.flatlaf.FlatLightLaf;
import tools.sqlclient.ui.MainFrame;
import tools.sqlclient.ui.UiStyle;

import javax.swing.SwingUtilities;

/**
 * 应用入口。使用 Swing + FlatLaf 提供类 Windows 11 清爽界面。
 */
public class Main {
    public static void main(String[] args) {
        // 设置 UTF-8
        System.setProperty("file.encoding", "UTF-8");
        FlatLightLaf.setup();
        UiStyle.installGlobalDefaults();
        SwingUtilities.invokeLater(() -> {
            MainFrame frame = new MainFrame();
            frame.setVisible(true);
        });
    }
}
