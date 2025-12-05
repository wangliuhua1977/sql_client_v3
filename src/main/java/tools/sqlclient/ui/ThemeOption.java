package tools.sqlclient.ui;

import javax.swing.LookAndFeel;
import java.util.Objects;
import java.util.function.Supplier;

public class ThemeOption {
    private final String id;
    private final String name;
    private final Supplier<LookAndFeel> lookAndFeelSupplier;

    public ThemeOption(String id, String name, Supplier<LookAndFeel> lookAndFeelSupplier) {
        this.id = Objects.requireNonNull(id, "id");
        this.name = Objects.requireNonNull(name, "name");
        this.lookAndFeelSupplier = Objects.requireNonNull(lookAndFeelSupplier, "lookAndFeelSupplier");
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public LookAndFeel createLookAndFeel() {
        return lookAndFeelSupplier.get();
    }
}
