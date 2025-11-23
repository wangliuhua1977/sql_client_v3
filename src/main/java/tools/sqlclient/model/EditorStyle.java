package tools.sqlclient.model;

/**
 * SQL 编辑器样式配置，可保存多套主题。
 */
public class EditorStyle {
    private final String name;
    private final int fontSize;
    private final String background;
    private final String foreground;
    private final String selection;
    private final String caret;
    private final String keyword;
    private final String stringColor;
    private final String commentColor;
    private final String numberColor;
    private final String operatorColor;
    private final String functionColor;
    private final String dataTypeColor;
    private final String identifierColor;
    private final String literalColor;
    private final String lineHighlight;
    private final String bracketColor;

    public EditorStyle(String name, int fontSize, String background, String foreground,
                       String selection, String caret, String keyword, String stringColor, String commentColor,
                       String numberColor, String operatorColor, String functionColor, String dataTypeColor,
                       String identifierColor, String literalColor, String lineHighlight, String bracketColor) {
        this.name = name;
        this.fontSize = fontSize;
        this.background = background;
        this.foreground = foreground;
        this.selection = selection;
        this.caret = caret;
        this.keyword = keyword;
        this.stringColor = stringColor;
        this.commentColor = commentColor;
        this.numberColor = numberColor;
        this.operatorColor = operatorColor;
        this.functionColor = functionColor;
        this.dataTypeColor = dataTypeColor;
        this.identifierColor = identifierColor;
        this.literalColor = literalColor;
        this.lineHighlight = lineHighlight;
        this.bracketColor = bracketColor;
    }

    public String getName() { return name; }
    public int getFontSize() { return fontSize; }
    public String getBackground() { return background; }
    public String getForeground() { return foreground; }
    public String getSelection() { return selection; }
    public String getCaret() { return caret; }
    public String getKeyword() { return keyword; }
    public String getStringColor() { return stringColor; }
    public String getCommentColor() { return commentColor; }
    public String getNumberColor() { return numberColor; }
    public String getOperatorColor() { return operatorColor; }
    public String getFunctionColor() { return functionColor; }
    public String getDataTypeColor() { return dataTypeColor; }
    public String getIdentifierColor() { return identifierColor; }
    public String getLiteralColor() { return literalColor; }
    public String getLineHighlight() { return lineHighlight; }
    public String getBracketColor() { return bracketColor; }

    public EditorStyle withName(String newName) {
        return new EditorStyle(newName, fontSize, background, foreground, selection, caret, keyword, stringColor, commentColor,
                numberColor, operatorColor, functionColor, dataTypeColor, identifierColor, literalColor, lineHighlight, bracketColor);
    }
}
