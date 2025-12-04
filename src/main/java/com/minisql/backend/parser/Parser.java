package com.minisql.backend.parser;

import java.util.ArrayList;
import java.util.List;

import com.minisql.backend.parser.statement.Abort;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Commit;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Describe;
import com.minisql.backend.parser.statement.Drop;
import com.minisql.backend.parser.statement.DropDatabase;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Show;
import com.minisql.backend.parser.statement.SingleExpression;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.parser.statement.CreateDatabase;
import com.minisql.backend.parser.statement.Use;
import com.minisql.backend.parser.statement.Where;
import com.minisql.backend.tbm.CompareOperator;
import com.minisql.backend.tbm.LogicOperator;
import com.minisql.backend.vm.IsolationLevel;
import com.minisql.common.Error;

public class Parser {
    public static Object parse(byte[] statement) throws Exception {
        Tokenizer tokenizer = new Tokenizer(statement);
        String token = tokenizer.peek();
        tokenizer.pop();

        Object stat = null;
        Exception statErr = null;
        try {
            switch(token.toLowerCase()) {
                case "begin":
                    stat = parseBegin(tokenizer);
                    break;
                case "commit":
                    stat = parseCommit(tokenizer);
                    break;
                case "abort":
                    stat = parseAbort(tokenizer);
                    break;
                case "rollback":
                    stat = parseAbort(tokenizer);
                    break;
                case "create":
                    stat = parseCreate(tokenizer);
                    break;
                case "drop":
                    stat = parseDrop(tokenizer);
                    break;
                case "select":
                    stat = parseSelect(tokenizer);
                    break;
                case "insert":
                    stat = parseInsert(tokenizer);
                    break;
                case "delete":
                    stat = parseDelete(tokenizer);
                    break;
                case "update":
                    stat = parseUpdate(tokenizer);
                    break;
                case "show":
                    stat = parseShow(tokenizer);
                    break;
                case "describe":
                    stat = parseDescribe(tokenizer);
                    break;
                case "desc":
                    stat = parseDescribe(tokenizer);
                    break;
                case "use":
                    stat = parseUse(tokenizer);
                    break;
                default:
                    throw Error.InvalidCommandException;
            }
        } catch(Exception e) {
            statErr = e;
        }
        try {
            if(!reachStatementEnd(tokenizer)) {
                byte[] errStat = tokenizer.errStat();
                statErr = new RuntimeException("Invalid statement: " + new String(errStat));
            }
        } catch(Exception e) {
            e.printStackTrace();
            byte[] errStat = tokenizer.errStat();
            statErr = new RuntimeException("Invalid statement: " + new String(errStat));
        }
        if(statErr != null) {
            throw statErr;
        }
        return stat;
    }

    private static Show parseShow(Tokenizer tokenizer) throws Exception {
        Show show = new Show();
        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            return show;
        } else if(";".equals(tmp)) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return show;
        } else if(eq(tmp, "tables")) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return show;
        } else if(eq(tmp, "databases")) {
            tokenizer.pop();
            show.target = Show.Target.DATABASES;
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return show;
        }
        throw Error.InvalidCommandException;
    }

    private static Describe parseDescribe(Tokenizer tokenizer) throws Exception {
        Describe describe = new Describe();
        String table = tokenizer.peek();
        if("".equals(table) || ";".equals(table)) {
            throw Error.InvalidCommandException;
        }
        if(!isName(table)) {
            throw Error.InvalidCommandException;
        }
        describe.tableName = table;
        tokenizer.pop();
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return describe;
    }

    private static Update parseUpdate(Tokenizer tokenizer) throws Exception {
        Update update = new Update();
        update.tableName = tokenizer.peek();
        tokenizer.pop();

        if(!eq(tokenizer.peek(), "set")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.fieldName = tokenizer.peek();
        tokenizer.pop();

        if(!"=".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        update.value = tokenizer.peek();
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            update.where = null;
            return update;
        } else if(";".equals(tmp)) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            update.where = null;
            return update;
        }

        update.where = parseWhere(tokenizer);
        return update;
    }

    private static Delete parseDelete(Tokenizer tokenizer) throws Exception {
        Delete delete = new Delete();

        if(!eq(tokenizer.peek(), "from")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        delete.tableName = tableName;
        tokenizer.pop();

        delete.where = parseWhere(tokenizer);
        return delete;
    }

    private static Insert parseInsert(Tokenizer tokenizer) throws Exception {
        Insert insert = new Insert();

        if(!eq(tokenizer.peek(), "into")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!eq(tokenizer.peek(), "values")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop(); // consume 'values'
        if(!"(".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop(); // consume '('

        List<String> values = new ArrayList<>();
        boolean hasValue = false;
        while(true) {
            String token = tokenizer.peek();
            if(")".equals(token)) {
                tokenizer.pop();
                break;
            }
            if(",".equals(token)) {
                tokenizer.pop();
                continue;
            }
            if("".equals(token) || ";".equals(token)) {
                throw Error.InvalidCommandException;
            }
            values.add(token);
            hasValue = true;
            tokenizer.pop();
        }
        if(!hasValue) {
            throw Error.InvalidCommandException;
        }
        if(";".equals(tokenizer.peek())) {
            tokenizer.pop();
        }
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        insert.values = values.toArray(new String[0]);
        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();

        List<String> fields = new ArrayList<>();
        String asterisk = tokenizer.peek();
        if("*".equals(asterisk)) {
            fields.add(asterisk);
            tokenizer.pop();
        } else {
            while(true) {
                String field = tokenizer.peek();
                if(!isName(field)) {
                    throw Error.InvalidCommandException;
                }
                fields.add(field);
                tokenizer.pop();
                if(",".equals(tokenizer.peek())) {
                    tokenizer.pop();
                } else {
                    break;
                }
            }
        }
        read.fields = fields.toArray(new String[fields.size()]);

        if(!eq(tokenizer.peek(), "from")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        read.tableName = tableName;
        tokenizer.pop();

        String tmp = tokenizer.peek();
        if("".equals(tmp)) {
            read.where = null;
            return read;
        } else if(";".equals(tmp)) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            read.where = null;
            return read;
        }

        read.where = parseWhere(tokenizer);
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!eq(tokenizer.peek(), "where")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOpToken = tokenizer.peek();
        if(";".equals(logicOpToken)) {
            tokenizer.pop();
            where.logicOp = "";
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return where;
        }
        LogicOperator lop = LogicOperator.from(logicOpToken);
        if(lop == LogicOperator.NONE && !"".equals(logicOpToken)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = lop.symbol;
        if(lop != LogicOperator.NONE) {
            tokenizer.pop();
        }

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String op = tokenizer.peek();
        CompareOperator operator = CompareOperator.from(op);
        tokenizer.pop();

        String value = tokenizer.peek();
        tokenizer.pop();
        return new SingleExpression(field, operator, value);
    }
    private static Object parseDrop(Tokenizer tokenizer) throws Exception {
        String target = tokenizer.peek();
        if(eq(target, "table")) {
            tokenizer.pop();
            return parseDropTable(tokenizer);
        } else if(eq(target, "database")) {
            tokenizer.pop();
            return parseDropDatabase(tokenizer);
        }
        throw Error.InvalidCommandException;
    }

    private static Drop parseDropTable(Tokenizer tokenizer) throws Exception {
        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        
        Drop drop = new Drop();
        drop.tableName = tableName;
        return drop;
    }

    private static DropDatabase parseDropDatabase(Tokenizer tokenizer) throws Exception {
        DropDatabase dropDatabase = new DropDatabase();
        String dbName = tokenizer.peek();
        if(!isName(dbName)) {
            throw Error.InvalidCommandException;
        }
        dropDatabase.databaseName = dbName;
        tokenizer.pop();
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return dropDatabase;
    }

    private static Object parseCreate(Tokenizer tokenizer) throws Exception {
        String target = tokenizer.peek();
        if(eq(target, "table")) {
            tokenizer.pop();
            return parseCreateTable(tokenizer);
        } else if(eq(target, "database")) {
            tokenizer.pop();
            return parseCreateDatabase(tokenizer);
        }
        throw Error.InvalidCommandException;
    }

    private static Create parseCreateTable(Tokenizer tokenizer) throws Exception {
        Create create = new Create();
        String name = tokenizer.peek();
        if(!isName(name)) {
            throw Error.InvalidCommandException;
        }
        create.tableName = name;
        tokenizer.pop(); // consume table name

        if(!"(".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop(); // consume '('

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        List<String> uniques = new ArrayList<>();
        List<String> indexes = new ArrayList<>();
        boolean primaryDeclared = false;

        while(true) {
            String token = tokenizer.peek();
            if(")".equals(token)) {
                tokenizer.pop();
                break;
            }
            if(",".equals(token)) {
                tokenizer.pop();
                continue;
            }

            if(eq(token, "index")) {
                // table-level index: INDEX (col1, col2, ...)
                tokenizer.pop();
                if(!"(".equals(tokenizer.peek())) {
                    throw Error.InvalidCommandException;
                }
                tokenizer.pop();
                boolean hasField = false;
                while(true) {
                    String fn = tokenizer.peek();
                    if(")".equals(fn)) {
                        if(!hasField) {
                            throw Error.InvalidCommandException;
                        }
                        tokenizer.pop();
                        break;
                    }
                    if(",".equals(fn)) {
                        tokenizer.pop();
                        continue;
                    }
                    if(!isName(fn)) {
                        throw Error.InvalidCommandException;
                    }
                    indexes.add(fn);
                    hasField = true;
                    tokenizer.pop();
                }
                // after table-level index, expect , or ) handled by loop
                continue;
            }

            // column definition: name type [PRIMARY KEY] [UNIQUE] [INDEX]
            if(!isName(token)) {
                throw Error.InvalidCommandException;
            }
            String fieldName = token;
            tokenizer.pop();

            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();

            boolean indexed = false;
            boolean unique = false;
            boolean primary = false;

            // consume optional constraints in any order
            while(true) {
                String next = tokenizer.peek();
                if(eq(next, "primary")) {
                    tokenizer.pop();
                    if(!eq(tokenizer.peek(), "key")) {
                        throw Error.InvalidCommandException;
                    }
                    tokenizer.pop();
                    primary = true;
                    unique = true;
                    indexed = true;
                    if(primaryDeclared) {
                        throw Error.InvalidCommandException;
                    }
                    primaryDeclared = true;
                    continue;
                } else if(eq(next, "unique")) {
                    tokenizer.pop();
                    unique = true;
                    indexed = true;
                    continue;
                } else if(eq(next, "index")) {
                    tokenizer.pop();
                    indexed = true;
                    continue;
                }
                break;
            }

            fNames.add(fieldName);
            fTypes.add(fieldType);
            if(unique || primary) {
                uniques.add(fieldName);
            }
            if(indexed) {
                indexes.add(fieldName);
            }

            // after a column def, expect ',' or ')' handled by loop
            String sep = tokenizer.peek();
            if(!",".equals(sep) && !")".equals(sep)) {
                throw Error.InvalidCommandException;
            }
        }

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }

        create.fieldName = fNames.toArray(new String[0]);
        create.fieldType = fTypes.toArray(new String[0]);
        create.unique = uniques.toArray(new String[0]);
        create.index = indexes.toArray(new String[0]);
        return create;
    }

    private static CreateDatabase parseCreateDatabase(Tokenizer tokenizer) throws Exception {
        String dbName = tokenizer.peek();
        if(!isName(dbName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        CreateDatabase createDatabase = new CreateDatabase();
        createDatabase.databaseName = dbName;
        return createDatabase;
    }

    private static boolean isType(String tp) {
        return ("int32".equalsIgnoreCase(tp) || "int64".equalsIgnoreCase(tp) ||
        "string".equalsIgnoreCase(tp));
    }

    private static Abort parseAbort(Tokenizer tokenizer) throws Exception {
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return new Abort();
    }

    private static Commit parseCommit(Tokenizer tokenizer) throws Exception {
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return new Commit();
    }

    private static Begin parseBegin(Tokenizer tokenizer) throws Exception {
        Begin begin = new Begin();
        String token = tokenizer.peek();
        if("".equals(token)) {
            return begin;
        }
        if(";".equals(token)) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return begin;
        }
        if(!eq(token, "isolation")) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String levelKeyword = tokenizer.peek();
        if(!eq(levelKeyword, "level")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String isolation = tokenizer.peek();
        if(eq(isolation, "read")) {
            tokenizer.pop();
            String committed = tokenizer.peek();
            if(!eq(committed, "committed")) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            begin.isolationLevel = IsolationLevel.READ_COMMITTED;
        } else if(eq(isolation, "repeatable")) {
            tokenizer.pop();
            String read = tokenizer.peek();
            if(!eq(read, "read")) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            begin.isolationLevel = IsolationLevel.REPEATABLE_READ;
        } else {
            throw Error.InvalidCommandException;
        }

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return begin;
    }

    private static Use parseUse(Tokenizer tokenizer) throws Exception {
        Use use = new Use();
        String dbName = tokenizer.peek();
        if(!isName(dbName)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        use.databaseName = dbName;
        return use;
    }

    /**
     * 判断name 是否合法
     * @param name 被判断的名称
     */
    private static boolean isName(String name) {
        return !(name.length() == 1 && !Tokenizer.isAlphaBeta(name.getBytes()[0]));
    }

    /**
     * 判断是否是语句的结束
     * @param tokenizer 分词器
     * @return true 表示是语句的结束
     */
    private static boolean reachStatementEnd(Tokenizer tokenizer) throws Exception {
        while(true) {
            String next = tokenizer.peek();
            if(";".equals(next)) {
                tokenizer.pop();
                continue;
            }
            return "".equals(next);
        }
    }

    /** 大小写不敏感比较 */
    private static boolean eq(String token, String keyword) {
        return keyword.equalsIgnoreCase(token);
    }
}
