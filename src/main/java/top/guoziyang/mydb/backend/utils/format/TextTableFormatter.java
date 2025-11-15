package top.guoziyang.mydb.backend.utils.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility for rendering ASCII tables similar to MySQL CLI output.
 */
public final class TextTableFormatter {
    private TextTableFormatter() {}

    public static String formatSingleColumn(String header, List<String> values) {
        List<String> headers = Collections.singletonList(header);
        List<List<String>> rows = new ArrayList<>();
        for (String value : values) {
            List<String> row = new ArrayList<>();
            row.add(value == null ? "" : value);
            rows.add(row);
        }
        return format(headers, rows);
    }

    public static String format(List<String> headers, List<List<String>> rows) {
        int columnCount = headers.size();
        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = headers.get(i).length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < columnCount; i++) {
                String value = row.get(i) == null ? "" : row.get(i);
                if(value.length() > widths[i]) {
                    widths[i] = value.length();
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        String horizontal = buildHorizontal(widths);
        sb.append(horizontal).append("\n");
        sb.append(buildRow(headers, widths)).append("\n");
        sb.append(horizontal).append("\n");
        for (List<String> row : rows) {
            List<String> normalized = new ArrayList<>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                String value = row.get(i);
                normalized.add(value == null ? "" : value);
            }
            sb.append(buildRow(normalized, widths)).append("\n");
        }
        sb.append(horizontal);
        return sb.toString();
    }

    private static String buildHorizontal(int[] widths) {
        StringBuilder sb = new StringBuilder("+");
        for (int width : widths) {
            sb.append(repeat('-', width + 2)).append("+");
        }
        return sb.toString();
    }

    private static String buildRow(List<String> values, int[] widths) {
        StringBuilder sb = new StringBuilder("|");
        for (int i = 0; i < values.size(); i++) {
            sb.append(" ");
            sb.append(padRight(values.get(i), widths[i]));
            sb.append(" |");
        }
        return sb.toString();
    }

    private static String padRight(String value, int width) {
        if(value == null) value = "";
        if(value.length() >= width) {
            return value;
        }
        StringBuilder sb = new StringBuilder(value);
        sb.append(repeat(' ', width - value.length()));
        return sb.toString();
    }

    private static String repeat(char ch, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(ch);
        }
        return sb.toString();
    }
}
