package tools.sqlclient.ui;

import com.formdev.flatlaf.FlatDarculaLaf;
import com.formdev.flatlaf.FlatDarkLaf;
import com.formdev.flatlaf.FlatIntelliJLaf;
import com.formdev.flatlaf.FlatLightLaf;
import tools.sqlclient.util.OperationLog;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.plaf.metal.MetalLookAndFeel;
import javax.swing.plaf.nimbus.NimbusLookAndFeel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class ThemeManager {
    private final List<ThemeOption> options;

    public ThemeManager() {
        List<ThemeOption> list = new ArrayList<>();
        list.add(new ThemeOption("flat-light", "默认（Flat Light）", FlatLightLaf::new));
        list.add(new ThemeOption("flat-dark", "暗色（Flat Dark）", FlatDarkLaf::new));
        list.add(new ThemeOption("intellij", "蓝灰 IntelliJ", FlatIntelliJLaf::new));
        list.add(new ThemeOption("darcula", "Darcula 深色", FlatDarculaLaf::new));
        list.add(new ThemeOption("nimbus", "经典 Nimbus", NimbusLookAndFeel::new));
        list.add(new ThemeOption("metal", "高对比 Metal", MetalLookAndFeel::new));
        options = Collections.unmodifiableList(list);
    }

    public List<ThemeOption> getOptions() {
        return options;
    }

    public Optional<ThemeOption> findById(String id) {
        return options.stream().filter(o -> o.getId().equals(id)).findFirst();
    }

    public void applyThemeAsync(ThemeOption option, Runnable afterApply) {
        if (option == null) {
            return;
        }
        SwingUtilities.invokeLater(() -> {
            try {
                UIManager.setLookAndFeel(option.createLookAndFeel());
                for (java.awt.Window window : java.awt.Window.getWindows()) {
                    SwingUtilities.updateComponentTreeUI(window);
                    window.invalidate();
                    window.validate();
                    window.repaint();
                }
            } catch (Exception ex) {
                OperationLog.log("切换主题失败: " + ex.getMessage());
            }
            if (afterApply != null) {
                afterApply.run();
            }
        });
    }
}
