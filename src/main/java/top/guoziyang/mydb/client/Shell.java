package top.guoziyang.mydb.client;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.checkerframework.checker.units.qual.s;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import top.guoziyang.mydb.backend.utils.format.ConsoleResultFormatter;
import top.guoziyang.mydb.backend.utils.format.ExecResult;
import top.guoziyang.mydb.backend.utils.format.ResultFormatter;

public class Shell {
    private static final String ANSI_RESET = "\u001B[0m";
    private static final String ANSI_CYAN = "\u001B[36m";
    private static final String PROMPT = ANSI_CYAN + "minisql> " + ANSI_RESET;
    private final Client client;
    private final ResultFormatter formatter = new ConsoleResultFormatter();

    public Shell(Client client) {
        this.client = client;
    }

    public void run() {
        Terminal terminal = null;
        try {
            terminal = TerminalBuilder.builder()
                    .system(true)
                    .build();
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .appName("mydb")
                    .history(new DefaultHistory())
                    .build();
            while(true) {
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
                statStr = statStr.trim();
                if(statStr.isEmpty()) {
                    continue;
                }
                if("exit".equalsIgnoreCase(statStr) || "quit".equalsIgnoreCase(statStr)) {
                    break;
                }
                try {
                    ExecResult res = client.execute(statStr.getBytes());
                    byte[] formatted = formatter.format(res);
                    System.out.println(new String(formatted, StandardCharsets.UTF_8));
                    System.out.println();
                } catch(Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize console", e);
        } finally {
            if(terminal != null) {
                try {
                    terminal.close();
                } catch (IOException ignore) {}
            }
            client.close();
        }
    }
}
