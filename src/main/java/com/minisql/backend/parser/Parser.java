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
            switch(token) {
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
        } else if("tables".equals(tmp)) {
            tokenizer.pop();
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return show;
        } else if("databases".equals(tmp)) {
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

        if(!"set".equals(tokenizer.peek())) {
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

        if(!"from".equals(tokenizer.peek())) {
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

        if(!"into".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String tableName = tokenizer.peek();
        if(!isName(tableName)) {
            throw Error.InvalidCommandException;
        }
        insert.tableName = tableName;
        tokenizer.pop();

        if(!"values".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> values = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String value = tokenizer.peek();
            if("".equals(value)) {
                break;
            } else if(";".equals(value)) {
                tokenizer.pop();
                if(!reachStatementEnd(tokenizer)) {
                    throw Error.InvalidCommandException;
                }
                break;
            } else {
                values.add(value);
            }
        }
        insert.values = values.toArray(new String[values.size()]);

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

        if(!"from".equals(tokenizer.peek())) {
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

        if(!"where".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        SingleExpression exp1 = parseSingleExp(tokenizer);
        where.singleExp1 = exp1;

        String logicOp = tokenizer.peek();
        if("".equals(logicOp)) {
            where.logicOp = logicOp;
            return where;
        }
        if(";".equals(logicOp)) {
            tokenizer.pop();
            where.logicOp = "";
            if(!reachStatementEnd(tokenizer)) {
                throw Error.InvalidCommandException;
            }
            return where;
        }
        if(!isLogicOp(logicOp)) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = logicOp;
        tokenizer.pop();

        SingleExpression exp2 = parseSingleExp(tokenizer);
        where.singleExp2 = exp2;

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return where;
    }

    private static SingleExpression parseSingleExp(Tokenizer tokenizer) throws Exception {
        SingleExpression exp = new SingleExpression();
        
        String field = tokenizer.peek();
        if(!isName(field)) {
            throw Error.InvalidCommandException;
        }
        exp.field = field;
        tokenizer.pop();

        String op = tokenizer.peek();
        if(!isCmpOp(op)) {
            throw Error.InvalidCommandException;
        }
        exp.compareOp = op;
        tokenizer.pop();

        exp.value = tokenizer.peek();
        tokenizer.pop();
        return exp;
    }

    private static boolean isCmpOp(String op) {
        return ("=".equals(op) || ">".equals(op) || "<".equals(op));
    }

    private static boolean isLogicOp(String op) {
        return ("and".equals(op) || "or".equals(op));
    }

    private static Object parseDrop(Tokenizer tokenizer) throws Exception {
        String target = tokenizer.peek();
        if("table".equals(target)) {
            tokenizer.pop();
            return parseDropTable(tokenizer);
        } else if("database".equals(target)) {
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
        if("table".equals(target)) {
            tokenizer.pop();
            return parseCreateTable(tokenizer);
        } else if("database".equals(target)) {
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

        List<String> fNames = new ArrayList<>();
        List<String> fTypes = new ArrayList<>();
        List<String> uniques = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if("(".equals(field)) {
                break;
            }

            if(!isName(field)) {
                throw Error.InvalidCommandException;
            }

            tokenizer.pop();
            String fieldType = tokenizer.peek();
            if(!isType(fieldType)) {
                throw Error.InvalidCommandException;
            }
            fNames.add(field);
            fTypes.add(fieldType);
            tokenizer.pop();

            if("unique".equals(tokenizer.peek())) {
                uniques.add(field);
                tokenizer.pop();
            }

            String next = tokenizer.peek();
            if(",".equals(next)) {
                continue;
            } else if("".equals(next)) {
                throw Error.TableNoIndexException;
            } else if("(".equals(next)) {
                break;
            } else {
                throw Error.InvalidCommandException;
            }
        }
        create.fieldName = fNames.toArray(new String[fNames.size()]);
        create.fieldType = fTypes.toArray(new String[fTypes.size()]);
        create.unique = uniques.toArray(new String[uniques.size()]);

        tokenizer.pop();
        if(!"index".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }

        List<String> indexes = new ArrayList<>();
        while(true) {
            tokenizer.pop();
            String field = tokenizer.peek();
            if(")".equals(field)) {
                break;
            }
            if(!isName(field)) {
                throw Error.InvalidCommandException;
            } else {
                indexes.add(field);
            }
        }
        create.index = indexes.toArray(new String[indexes.size()]);
        tokenizer.pop();

        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
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
        return ("int32".equals(tp) || "int64".equals(tp) ||
        "string".equals(tp));
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
        if(!"isolation".equals(token)) {
            throw Error.InvalidCommandException;
        }

        tokenizer.pop();
        String levelKeyword = tokenizer.peek();
        if(!"level".equals(levelKeyword)) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        String isolation = tokenizer.peek();
        if("read".equals(isolation)) {
            tokenizer.pop();
            String committed = tokenizer.peek();
            if(!"committed".equals(committed)) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            begin.isolationLevel = IsolationLevel.READ_COMMITTED;
        } else if("repeatable".equals(isolation)) {
            tokenizer.pop();
            String read = tokenizer.peek();
            if(!"read".equals(read)) {
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
}
