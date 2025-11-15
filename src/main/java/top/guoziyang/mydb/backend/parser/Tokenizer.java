package top.guoziyang.mydb.backend.parser;

import top.guoziyang.mydb.common.Error;

/**
 * Tokenizer 用于将 SQL 语句按顺序切分为 token（词元）。
 * 它支持：
 * <ul>
 *     <li>标识符（字母/数字/下划线）</li>
 *     <li>字符串字面量（单引号/双引号）</li>
 *     <li>符号（><=*,();等）</li>
 *     <li>跳过空白字符</li>
 * </ul>
 *
 * Tokenizer 会维护当前位置、错误状态、以及一个“peek 缓冲”。
 * <p>
 * 特点：
 * <ul>
 *     <li>peek() 预读 token，但不移动游标</li>
 *     <li>pop() 消费 token，使下次 peek() 获取下一个</li>
 *     <li>错误状态会在后续继续抛出（err 机制）</li>
 * </ul>
 */
public class Tokenizer {

    /** 原始 SQL 字节序列 */
    private byte[] stat;

    /** 当前读取位置（cursor） */
    private int pos;

    /** peek 后缓存的 token */
    private String currentToken;

    /** 是否需要刷新（读取新 token） */
    private boolean flushToken;

    /** 解析过程中遇到的错误，如果发生则持续抛出 */
    private Exception err;

    /**
     * 创建 Tokenizer，并绑定 SQL 字节数组作为输入源。
     *
     * @param stat SQL 命令对应的 byte[] 数组
     */
    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    /**
     * 返回当前 token（不推进游标）。若 token 尚未读取则自动调用 next()。
     *
     * @return 当前 token 字符串
     * @throws Exception 当解析发生错误时抛出
     */
    public String peek() throws Exception {
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try {
                token = next();
            } catch(Exception e) {
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    /**
     * 消费掉当前 token，使下次 peek() 返回后续 token。
     */
    public void pop() {
        flushToken = true;
    }

    /**
     * 构造一个标记错误位置的 SQL 字节流，用于错误提示（在 pos 位置前插入 "<< "）。
     *
     * @return 标注错误位置的 byte[]
     */
    public byte[] errStat() {
        byte[] res = new byte[stat.length+3];
        System.arraycopy(stat, 0, res, 0, pos);
        System.arraycopy("<< ".getBytes(), 0, res, pos, 3);
        System.arraycopy(stat, pos, res, pos+3, stat.length-pos);
        return res;
    }

    /** 推进游标一位 */
    private void popByte() {
        pos ++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    /** 查看当前位置的 byte，不移动游标 */
    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    /**
     * 读取下一个 token 并推进游标。
     */
    private String next() throws Exception {
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    /**
     * 顶层状态机入口：跳过空白，根据首字节决定进入不同子解析状态。
     */
    private String nextMetaState() throws Exception {
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }
            if(!isBlank(b)) {
                break;
            }
            popByte();
        }
        byte b = peekByte();
        if(isSymbol(b)) {
            popByte();
            return new String(new byte[]{b});
        } else if(b == '"' || b == '\'') {
            return nextQuoteState();
        } else if(isAlphaBeta(b) || isDigit(b)) {
            return nextTokenState();
        } else {
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    /**
     * 解析标识符（字母/数字/下划线组成）。
     */
    private String nextTokenState() throws Exception {
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null || !(isAlphaBeta(b) || isDigit(b) || b == '_')) {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                return sb.toString();
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
    }

    /** 判断是否数字 */
    static boolean isDigit(byte b) {
        return (b >= '0' && b <= '9');
    }

    /** 判断是否英文字母 */
    static boolean isAlphaBeta(byte b) {
        return ((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z'));
    }

    /**
     * 解析引号内的字符串，支持单引号和双引号。
     * 若引号未闭合则抛错。
     */
    private String nextQuoteState() throws Exception {
        byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    /** 判断是否为 SQL 特殊符号 */
    static boolean isSymbol(byte b) {
        return (b == '>' || b == '<' || b == '=' || b == '*' ||
                b == ',' || b == '(' || b == ')' || b == ';');
    }

    /** 判断是否空白字符（空格/tab/换行） */
    static boolean isBlank(byte b) {
        return (b == '\n' || b == ' ' || b == '\t');
    }
}
