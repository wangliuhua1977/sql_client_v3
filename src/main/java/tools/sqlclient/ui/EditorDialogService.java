package tools.sqlclient.ui;

import javax.swing.*;
import javax.swing.table.TableModel;
import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Window;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.InputEvent;
import java.awt.event.ItemListener;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;

/**
 * 结果集单元格的完整内容编辑器服务。
 */
public class EditorDialogService {

    public Result showDialog(Window owner, String title, String initialText) {
        EditorDialog dialog = new EditorDialog(owner, title, initialText);
        dialog.setVisible(true);
        return dialog.getResult();
    }

    public void apply(TableModel model, int row, int column, String newValue) {
        if (model == null) {
            return;
        }
        model.setValueAt(newValue, row, column);
    }

    public record Result(boolean confirmed, String value) {
    }

    private static final class EditorDialog extends JDialog {
        private final JTextArea textArea;
        private final JCheckBox wrapCheck;
        private final JButton okButton;
        private final JButton cancelButton;
        private final ActionListener okListener;
        private final ActionListener cancelListener;
        private final ItemListener wrapListener;
        private final WindowAdapter closeHandler;
        private Result result = new Result(false, null);

        private EditorDialog(Window owner, String title, String initialText) {
            super(owner, title, ModalityType.APPLICATION_MODAL);
            setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);

            textArea = new JTextArea(initialText == null ? "" : initialText);
            textArea.setLineWrap(true);
            textArea.setWrapStyleWord(true);
            JScrollPane scrollPane = new JScrollPane(textArea);

            wrapCheck = new JCheckBox("自动换行", true);
            wrapListener = e -> {
                boolean enabled = wrapCheck.isSelected();
                textArea.setLineWrap(enabled);
                textArea.setWrapStyleWord(enabled);
            };
            wrapCheck.addItemListener(wrapListener);

            okButton = new JButton("确定");
            cancelButton = new JButton("取消");

            okListener = e -> {
                result = new Result(true, textArea.getText());
                dispose();
            };
            cancelListener = e -> {
                result = new Result(false, null);
                dispose();
            };
            okButton.addActionListener(okListener);
            cancelButton.addActionListener(cancelListener);

            closeHandler = new WindowAdapter() {
                @Override
                public void windowClosing(java.awt.event.WindowEvent e) {
                    cancelButton.doClick();
                }
            };
            addWindowListener(closeHandler);

            JPanel buttonPanel = new JPanel(new FlowLayout(FlowLayout.RIGHT, 8, 6));
            buttonPanel.add(okButton);
            buttonPanel.add(cancelButton);

            JPanel south = new JPanel(new BorderLayout());
            south.add(wrapCheck, BorderLayout.WEST);
            south.add(buttonPanel, BorderLayout.EAST);

            setLayout(new BorderLayout());
            add(scrollPane, BorderLayout.CENTER);
            add(south, BorderLayout.SOUTH);

            getRootPane().setDefaultButton(okButton);
            installKeyBindings();

            setPreferredSize(new Dimension(800, 600));
            pack();
            setLocationRelativeTo(owner);
        }

        private void installKeyBindings() {
            InputMap inputMap = getRootPane().getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
            ActionMap actionMap = getRootPane().getActionMap();
            inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), "cancel");
            actionMap.put("cancel", new AbstractAction() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    cancelButton.doClick();
                }
            });
            inputMap.put(KeyStroke.getKeyStroke(KeyEvent.VK_ENTER, InputEvent.CTRL_DOWN_MASK), "confirm");
            actionMap.put("confirm", new AbstractAction() {
                @Override
                public void actionPerformed(ActionEvent e) {
                    okButton.doClick();
                }
            });
        }

        private Result getResult() {
            return result;
        }

        @Override
        public void dispose() {
            okButton.removeActionListener(okListener);
            cancelButton.removeActionListener(cancelListener);
            wrapCheck.removeItemListener(wrapListener);
            removeWindowListener(closeHandler);
            super.dispose();
        }
    }
}
