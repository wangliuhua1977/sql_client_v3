package tools.sqlclient.ui;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import javax.swing.JFrame;
import javax.swing.JPanel;
import java.awt.GraphicsEnvironment;
import java.awt.Window;

import static org.junit.jupiter.api.Assertions.assertSame;

class WindowOwnerResolverTest {

    @Test
    void resolveReturnsWindowAncestorWhenAvailable() {
        Assumptions.assumeFalse(GraphicsEnvironment.isHeadless(), "Headless environment skips Swing window test.");

        JFrame frame = new JFrame("owner");
        try {
            JPanel panel = new JPanel();
            frame.setContentPane(panel);

            Window resolved = WindowOwnerResolver.resolve(panel, null);

            assertSame(frame, resolved);
        } finally {
            frame.dispose();
        }
    }
}
