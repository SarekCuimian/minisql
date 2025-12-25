package com.minisql.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import com.minisql.common.ConsoleResultFormatter;
import com.minisql.common.ExecResult;
import com.minisql.common.ResultFormatter;

public class Shell {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String PROMPT = ANSI_CYAN + "sql> " + ANSI_RESET;
    private static final String CONT_PROMPT = ANSI_CYAN + "  -> " + ANSI_RESET;
    private final Client client;
    private final ResultFormatter formatter = new ConsoleResultFormatter();

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .appName("MiniSQL")
                    .history(new MergedHistory())
                    // 不忽略以空格开头的命令，保证历史可用
                    .option(LineReader.Option.HISTORY_IGNORE_SPACE, false)
                    .build();
            while (true) {
                String statStr;
                try {
                    statStr = reader.readLine(PROMPT);
                } catch (UserInterruptException ignore) {
                    // 用户按下 Ctrl+C，保持会话继续
                    continue;
                } catch (EndOfFileException eof) {
                    // Ctrl+D 退出
                    break;
                }
                if (statStr == null) continue;

                // 累积输入，遇到分号结尾再执行，模仿 MySQL CLI 行为
                StringBuilder buffer = new StringBuilder();
                String current = statStr;
                while (true) {
                    String trimmed = current.trim();
                    // 仅在缓冲为空时响应退出命令
                    if (buffer.length() == 0 &&
                            ("exit".equalsIgnoreCase(trimmed) || "quit".equalsIgnoreCase(trimmed))) {
                        return;
                    }
                    if (!trimmed.isEmpty()) {
                        buffer.append(current);
                        // 单行不必强制换行，但多行保留换行，便于调试
                        if (!trimmed.endsWith(";")) {
                            buffer.append('\n');
                        }
                    }
                    if (isCompleteStatement(buffer.toString())) {
                        String sql = buffer.toString().trim();
                        if (sql.isEmpty()) {
                            break;
                        }
                        try {
                            ExecResult res = client.execute(sql.getBytes());
                            byte[] formatted = formatter.format(res);
                            System.out.println(new String(formatted, StandardCharsets.UTF_8));
                            System.out.println();
                        } catch (Exception e) {
                            System.out.println(e.getMessage());
                        }
                        break;
                    }
                    try {
                        current = reader.readLine(CONT_PROMPT);
                    } catch (UserInterruptException ignore) {
                        // Ctrl+C 清空当前缓冲，回到主提示符
                        resetHistory(reader);
                        buffer.setLength(0);
                        break;
                    } catch (EndOfFileException eof) {
                        return;
                    }
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize console", e);
        } finally {
            client.close();
        }
    }

    /**
     * 判断是否输入了完整语句：最后一个非空白字符为分号，且不在引号内。
     */
    private boolean isCompleteStatement(String sql) {
        if (sql == null || sql.isEmpty()) return false;
        boolean inSingle = false, inDouble = false;
        char lastNonBlank = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && !inDouble) {
                inSingle = !inSingle;
            } else if (c == '\"' && !inSingle) {
                inDouble = !inDouble;
            }
            if (!Character.isWhitespace(c)) {
                lastNonBlank = c;
            }
        }
        return !inSingle && !inDouble && lastNonBlank == ';';
    }

    private void resetHistory(LineReader reader) {
        if(reader.getHistory() instanceof MergedHistory) {
            ((MergedHistory) reader.getHistory()).resetPending();
        }
    }

    /**
     * 合并多行输入为单行存入历史，模拟 MySQL CLI 压缩行为。
     */
    private static class MergedHistory extends DefaultHistory {
        private final StringBuilder pending = new StringBuilder();

        @Override
        public void add(Instant time, String line) {
            if(line == null) return;
            String trimmed = line.trim();
            if(trimmed.isEmpty()) return;
            pending.append(line);
            if(!trimmed.endsWith(";")) {
                pending.append(' ');
                return;
            }
            String merged = collapse(pending.toString());
            pending.setLength(0);
            super.add(time, merged);
        }

        void resetPending() {
            pending.setLength(0);
        }

        private String collapse(String sql) {
            String s = sql.replaceAll("\\s+", " ").trim();
            // 去掉末尾分号前的空格（例如 "id ;" -> "id;")
            s = s.replaceAll("\\s*;\\s*$", ";");
            return s;
        }
    }
}
