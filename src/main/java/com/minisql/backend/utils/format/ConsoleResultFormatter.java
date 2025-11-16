package com.minisql.backend.utils.format;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * 控制台结果格式化器
 */
public class ConsoleResultFormatter implements ResultFormatter {

    @Override
    public byte[] format(ExecResult result) {
        if (result.getType() == ExecResult.Type.RESULT) {
            return formatResultSet(result);
        }
        return formatOK(result);
    }

    private byte[] formatResultSet(ExecResult result) {
        StringBuilder sb = new StringBuilder();
        byte[] payload = result.getPayload();
        if (payload.length > 0) {
            sb.append(new String(payload, StandardCharsets.UTF_8)).append("\n");
        }
        int rows = Math.max(result.getResultRows(), 0);
        String summary = rows + (rows == 1 ? " row" : " rows") +
                " in set (" + formatSeconds(result.getElapsedNanos()) + " sec)";
        sb.append(summary);
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] formatOK(ExecResult result) {
        StringBuilder sb = new StringBuilder("Query OK");
        int affectedRows = result.getAffectedRows();
        if (affectedRows >= 0) {
            sb.append(", ")
                    .append(affectedRows)
                    .append(affectedRows == 1 ? " row" : " rows")
                    .append(" affected");
        }
        sb.append(" (").append(formatSeconds(result.getElapsedNanos())).append(" sec)");
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String formatSeconds(long nanos) {
        double seconds = nanos / 1_000_000_000d;
        return String.format(Locale.ROOT, "%.2f", seconds);
    }
}
