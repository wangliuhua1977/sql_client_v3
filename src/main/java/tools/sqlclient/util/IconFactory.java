package tools.sqlclient.util;

import javax.imageio.ImageIO;
import javax.swing.Icon;
import javax.swing.ImageIcon;
import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.InputStream;

import tools.sqlclient.util.OperationLog;

/**
 * 统一的图标工厂：优先从资源加载，如缺失则使用矢量绘制回退图标。
 */
public final class IconFactory {
    private static final int DEFAULT_ICON_SIZE = 20;
    public static final String RUN_ICON_PATH = "/icons/run.png";
    public static final String RUN_DISABLED_ICON_PATH = "/icons/run_disabled.png";
    public static final String STOP_ICON_PATH = "/icons/stop.png";
    public static final String STOP_DISABLED_ICON_PATH = "/icons/stop_disabled.png";

    private IconFactory() {
    }

    public static Icon createRunIcon(boolean enabled) {
        return loadOrDraw(enabled ? RUN_ICON_PATH : RUN_DISABLED_ICON_PATH,
                () -> drawPlayIcon(enabled ? new Color(46, 170, 220) : new Color(160, 160, 160)));
    }

    public static Icon createStopIcon(boolean enabled) {
        return loadOrDraw(enabled ? STOP_ICON_PATH : STOP_DISABLED_ICON_PATH,
                () -> drawStopIcon(enabled ? new Color(226, 80, 65) : new Color(160, 160, 160)));
    }

    private static Icon loadOrDraw(String path, java.util.function.Supplier<Icon> fallback) {
        try (InputStream in = IconFactory.class.getResourceAsStream(path)) {
            if (in != null) {
                BufferedImage img = ImageIO.read(in);
                if (img != null) {
                    return new ImageIcon(img);
                }
            }
        } catch (Exception ex) {
            OperationLog.log("图标加载失败: " + path + " -> " + ex.getMessage());
        }
        return fallback.get();
    }

    private static Icon drawPlayIcon(Color color) {
        int size = DEFAULT_ICON_SIZE;
        BufferedImage image = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2 = image.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        int offset = 3;
        int[] xs = {offset, offset, size - offset};
        int[] ys = {offset, size - offset, size / 2};
        g2.setColor(color);
        g2.fillPolygon(xs, ys, 3);
        g2.dispose();
        return new ImageIcon(image);
    }

    private static Icon drawStopIcon(Color color) {
        int size = DEFAULT_ICON_SIZE;
        BufferedImage image = new BufferedImage(size, size, BufferedImage.TYPE_INT_ARGB);
        Graphics2D g2 = image.createGraphics();
        g2.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON);
        int padding = 4;
        g2.setColor(color);
        g2.fillRoundRect(padding, padding, size - padding * 2, size - padding * 2, 4, 4);
        g2.dispose();
        return new ImageIcon(image);
    }
}
