package tools.sqlclient.ui.findreplace;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class FindReplaceEngine {

    public static final class Match {
        private final int start;
        private final int end;

        public Match(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int start() {
            return start;
        }

        public int end() {
            return end;
        }
    }

    public static final class FindAllResult {
        private final List<Match> matches;
        private final int total;
        private final boolean tooMany;
        private final String errorMessage;

        public FindAllResult(List<Match> matches, int total, boolean tooMany, String errorMessage) {
            this.matches = matches;
            this.total = total;
            this.tooMany = tooMany;
            this.errorMessage = errorMessage;
        }

        public List<Match> matches() {
            return matches;
        }

        public int total() {
            return total;
        }

        public boolean tooMany() {
            return tooMany;
        }

        public String errorMessage() {
            return errorMessage;
        }
    }

    public FindResult find(String text, int startPos, FindOptions options) {
        if (text == null || text.isEmpty()) {
            return FindResult.notFound(false);
        }
        if (options.query().isEmpty()) {
            return FindResult.notFound(false);
        }
        if (options.direction() == FindOptions.Direction.FORWARD) {
            return findForward(text, startPos, options);
        }
        return findBackward(text, startPos, options);
    }

    public FindAllResult findAll(String text, FindOptions options, int limit) {
        List<Match> matches = new ArrayList<>();
        if (text == null || text.isEmpty() || options.query().isEmpty()) {
            return new FindAllResult(matches, 0, false, null);
        }
        try {
            int count = 0;
            if (options.regex()) {
                Pattern pattern = compile(options);
                Matcher matcher = pattern.matcher(text);
                while (matcher.find()) {
                    int start = matcher.start();
                    int end = matcher.end();
                    if (options.wholeWord() && !isWholeWord(text, start, end)) {
                        continue;
                    }
                    count++;
                    if (matches.size() < limit) {
                        matches.add(new Match(start, end));
                    }
                }
            } else {
                String haystack = options.caseSensitive() ? text : text.toLowerCase(Locale.ROOT);
                String needle = options.caseSensitive() ? options.query() : options.query().toLowerCase(Locale.ROOT);
                int idx = 0;
                while (idx <= haystack.length()) {
                    idx = haystack.indexOf(needle, idx);
                    if (idx < 0) {
                        break;
                    }
                    int end = idx + needle.length();
                    if (options.wholeWord() && !isWholeWord(text, idx, end)) {
                        idx = idx + 1;
                        continue;
                    }
                    count++;
                    if (matches.size() < limit) {
                        matches.add(new Match(idx, end));
                    }
                    idx = end;
                }
            }
            boolean tooMany = count > limit;
            return new FindAllResult(matches, count, tooMany, null);
        } catch (PatternSyntaxException ex) {
            return new FindAllResult(matches, 0, false, ex.getDescription());
        }
    }

    private FindResult findForward(String text, int startPos, FindOptions options) {
        try {
            int start = Math.max(0, Math.min(startPos, text.length()));
            Match match = findForwardMatch(text, start, options);
            boolean wrapped = false;
            if (match == null && options.wrap()) {
                match = findForwardMatch(text, 0, options);
                wrapped = match != null;
            }
            if (match == null) {
                return FindResult.notFound(wrapped);
            }
            return FindResult.found(match.start(), match.end(), -1, -1, wrapped);
        } catch (PatternSyntaxException ex) {
            return FindResult.error(ex.getDescription());
        }
    }

    private FindResult findBackward(String text, int startPos, FindOptions options) {
        try {
            int start = Math.max(0, Math.min(startPos, text.length()));
            Match match = findBackwardMatch(text, start, options);
            boolean wrapped = false;
            if (match == null && options.wrap()) {
                match = findBackwardMatch(text, text.length(), options);
                wrapped = match != null;
            }
            if (match == null) {
                return FindResult.notFound(wrapped);
            }
            return FindResult.found(match.start(), match.end(), -1, -1, wrapped);
        } catch (PatternSyntaxException ex) {
            return FindResult.error(ex.getDescription());
        }
    }

    private Match findForwardMatch(String text, int startPos, FindOptions options) {
        if (options.regex()) {
            Pattern pattern = compile(options);
            Matcher matcher = pattern.matcher(text);
            int idx = startPos;
            while (idx <= text.length() && matcher.find(idx)) {
                int start = matcher.start();
                int end = matcher.end();
                if (!options.wholeWord() || isWholeWord(text, start, end)) {
                    return new Match(start, end);
                }
                idx = start + 1;
            }
            return null;
        }
        String haystack = options.caseSensitive() ? text : text.toLowerCase(Locale.ROOT);
        String needle = options.caseSensitive() ? options.query() : options.query().toLowerCase(Locale.ROOT);
        int idx = startPos;
        while (idx <= haystack.length()) {
            idx = haystack.indexOf(needle, idx);
            if (idx < 0) {
                return null;
            }
            int end = idx + needle.length();
            if (!options.wholeWord() || isWholeWord(text, idx, end)) {
                return new Match(idx, end);
            }
            idx = idx + 1;
        }
        return null;
    }

    private Match findBackwardMatch(String text, int startPos, FindOptions options) {
        if (options.regex()) {
            Pattern pattern = compile(options);
            Matcher matcher = pattern.matcher(text);
            Match last = null;
            while (matcher.find()) {
                int start = matcher.start();
                int end = matcher.end();
                if (start >= startPos) {
                    break;
                }
                if (options.wholeWord() && !isWholeWord(text, start, end)) {
                    continue;
                }
                last = new Match(start, end);
            }
            return last;
        }
        String haystack = options.caseSensitive() ? text : text.toLowerCase(Locale.ROOT);
        String needle = options.caseSensitive() ? options.query() : options.query().toLowerCase(Locale.ROOT);
        int idx = Math.min(startPos, haystack.length());
        while (idx >= 0) {
            idx = haystack.lastIndexOf(needle, idx - 1);
            if (idx < 0) {
                return null;
            }
            int end = idx + needle.length();
            if (!options.wholeWord() || isWholeWord(text, idx, end)) {
                return new Match(idx, end);
            }
        }
        return null;
    }

    private Pattern compile(FindOptions options) {
        int flags = Pattern.MULTILINE;
        if (!options.caseSensitive()) {
            flags |= Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE;
        }
        return Pattern.compile(options.query(), flags);
    }

    private boolean isWholeWord(String text, int start, int end) {
        boolean leftOk = start <= 0 || !isWordChar(text.charAt(start - 1));
        boolean rightOk = end >= text.length() || !isWordChar(text.charAt(end));
        return leftOk && rightOk;
    }

    private boolean isWordChar(char ch) {
        return ch == '_' || Character.isLetterOrDigit(ch);
    }
}
