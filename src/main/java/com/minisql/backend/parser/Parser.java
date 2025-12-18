package com.minisql.backend.parser;

import java.util.ArrayList;
import java.util.List;

import com.minisql.backend.parser.statement.Abort;
import com.minisql.backend.parser.statement.Begin;
import com.minisql.backend.parser.statement.Commit;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Aggregate;
import com.minisql.backend.aggregator.AggregateFunc;
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
import com.minisql.backend.parser.statement.condition.Condition;
import com.minisql.backend.parser.statement.condition.Operand;
import com.minisql.backend.parser.statement.operator.CompareOperator;
import com.minisql.backend.parser.statement.operator.LogicOperator;
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
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
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
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
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

        // 可选列名列表
        List<String> columns = null;
        if("(".equals(tokenizer.peek())) {
            tokenizer.pop(); // consume '('
            columns = new ArrayList<>();
            boolean hasColumn = false;
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
                if(!isName(token)) {
                    throw Error.InvalidCommandException;
                }
                columns.add(token);
                hasColumn = true;
                tokenizer.pop();
            }
            if(!hasColumn) {
                throw Error.InvalidCommandException;
            }
        }

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
        if(columns != null) {
            insert.columns = columns.toArray(new String[0]);
        }
        insert.values = values.toArray(new String[0]);
        return insert;
    }

    private static Select parseSelect(Tokenizer tokenizer) throws Exception {
        Select read = new Select();
        List<String> fields = new ArrayList<>();
        List<Aggregate> aggregates = new ArrayList<>();
        List<Select.Item> projection = new ArrayList<>();
        while(true) {
            String token = tokenizer.peek();
            if(isAggregateFunc(token)) {
                Aggregate agg = parseAggregate(tokenizer);
                String alias = parseAliasIfPresent(tokenizer);
                aggregates.add(agg);
                projection.add(Select.Item.ofAggregate(agg, alias));
            } else {
                if(!"*".equals(token) && !isName(token)) {
                    throw Error.InvalidCommandException;
                }
                String column = token;
                tokenizer.pop();
                String alias = parseAliasIfPresent(tokenizer);
                fields.add(column);
                projection.add(Select.Item.ofColumn(column, alias));
            }
            if(",".equals(tokenizer.peek())) {
                tokenizer.pop();
                continue;
            }
            break;
        }
        if(!fields.isEmpty()) {
            read.fields = fields.toArray(new String[0]);
        }
        if(!aggregates.isEmpty()) {
            read.aggregates = aggregates.toArray(new Aggregate[0]);
        }
        read.projection = projection.toArray(new Select.Item[0]);


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

        if(eq(tokenizer.peek(), "where")) {
            read.where = parseWhere(tokenizer);
        } else {
            read.where = null;
        }

        if(eq(tokenizer.peek(), "group")) {
            read.groupBy = parseGroupBy(tokenizer);
        }
        if(eq(tokenizer.peek(), "having")) {
            tokenizer.pop();
            read.having = parseHaving(tokenizer, read);
        }
        if(";".equals(tokenizer.peek())) {
            tokenizer.pop();
        }
        if(!reachStatementEnd(tokenizer)) {
            throw Error.InvalidCommandException;
        }
        return read;
    }

    private static Where parseWhere(Tokenizer tokenizer) throws Exception {
        Where where = new Where();

        if(!eq(tokenizer.peek(), "where")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();

        where.singleExp1 = parseSingleExp(tokenizer);

        String logicOpToken = tokenizer.peek();
        if(";".equals(logicOpToken)) {
            tokenizer.pop();
            where.logicOp = "";
            return where;
        }
        if("".equals(logicOpToken)) {
            where.logicOp = "";
            return where;
        }
        // 兼容 SELECT ... WHERE ... GROUP BY ... 场景，where 后直接进入 group
        if(eq(logicOpToken, "group")) {
            where.logicOp = "";
            return where;
        }
        LogicOperator lop = LogicOperator.from(logicOpToken);
        if(lop == LogicOperator.NONE) {
            throw Error.InvalidCommandException;
        }
        where.logicOp = lop.symbol();
        if(lop != LogicOperator.NONE) {
            tokenizer.pop();
            where.singleExp2 = parseSingleExp(tokenizer);
        } else {
            where.singleExp2 = null;
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

    private static String[] parseGroupBy(Tokenizer tokenizer) throws Exception {
        if(!eq(tokenizer.peek(), "group")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        if(!eq(tokenizer.peek(), "by")) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        List<String> groupingColumns = new ArrayList<>();
        boolean hasGroupingColumns = false;
        while(true) {
            String col = tokenizer.peek();
            if(!isName(col)) {
                throw Error.InvalidCommandException;
            }
            groupingColumns.add(col);
            hasGroupingColumns = true;
            tokenizer.pop();
            if(",".equals(tokenizer.peek())) {
                tokenizer.pop();
                continue;
            }
            break;
        }
        if(!hasGroupingColumns) {
            throw Error.InvalidCommandException;
        }
        return groupingColumns.toArray(new String[0]);
    }

    private static Condition parseHaving(Tokenizer tokenizer, Select select) throws Exception {
        if(select.groupBy == null || select.groupBy.length == 0) {
            throw Error.InvalidCommandException;
        }
        return parseHavingOr(tokenizer, select);
    }

    private static Condition parseHavingOr(Tokenizer tokenizer, Select select) throws Exception {
        Condition left = parseHavingAnd(tokenizer, select);
        while(true) {
            String token = tokenizer.peek();
            LogicOperator lop = LogicOperator.from(token);
            if(lop != LogicOperator.OR) break;
            tokenizer.pop();
            Condition right = parseHavingAnd(tokenizer, select);
            left = Condition.ofBinary(left, lop, right);
        }
        return left;
    }

    private static Condition parseHavingAnd(Tokenizer tokenizer, Select select) throws Exception {
        Condition left = parseHavingFactor(tokenizer, select);
        while(true) {
            String token = tokenizer.peek();
            LogicOperator lop = LogicOperator.from(token);
            if(lop != LogicOperator.AND) break;
            tokenizer.pop();
            Condition right = parseHavingFactor(tokenizer, select);
            left = Condition.ofBinary(left, lop, right);
        }
        return left;
    }

    /**
     * 匹配 having 后使用 () 指定的优先级
     */
    private static Condition parseHavingFactor(Tokenizer tokenizer, Select select) throws Exception {
        if("(".equals(tokenizer.peek())) {
            tokenizer.pop();
            Condition inner = parseHavingOr(tokenizer, select);
            if(!")".equals(tokenizer.peek())) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            return inner;
        }
        return parseHavingPredicate(tokenizer, select);
    }

    private static Condition parseHavingPredicate(Tokenizer tokenizer, Select select) throws Exception {
        Operand left = parseHavingOperand(tokenizer, select);
        String opToken = tokenizer.peek();
        CompareOperator cop = CompareOperator.from(opToken);
        if(cop == null) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        Operand right = parseHavingOperand(tokenizer, select);
        return Condition.ofPredicate(left, cop, right);
    }

    private static Operand parseHavingOperand(Tokenizer tokenizer, Select select) throws Exception {
        String token = tokenizer.peek();
        if(isAggregateFunc(token)) {
            // func ( arg )
            String funcToken = token;
            tokenizer.pop();
            if(!"(".equals(tokenizer.peek())) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            String arg = tokenizer.peek();
            boolean star = "*".equals(arg);
            if(star) {
                tokenizer.pop();
            } else {
                if(!isName(arg)) {
                    throw Error.InvalidCommandException;
                }
                tokenizer.pop();
            }
            if(!")".equals(tokenizer.peek())) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            int aggIdx = getAggregateIndex(select, funcToken, star ? null : arg);
            if(aggIdx < 0) {
                throw Error.InvalidCommandException;
            }
            return Operand.ofAggregate(aggIdx);
        }
        if(isName(token)) {
            // 先看是否是 groupBy 列
            int groupIdx = getGroupingColumnIndex(select, token);
            if(groupIdx >= 0) {
                tokenizer.pop();
                return Operand.ofColumn(groupIdx);
            }
            // 分组列别名（投影中列的别名且原列在 groupBy 中）
            String column = getColumnByAlias(select, token);
            if(column != null) {
                tokenizer.pop();
                int idx = getGroupingColumnIndex(select, column);
                return Operand.ofColumn(idx);
            }
            // 聚合别名
            int aggIdx = getAggregateByAlias(select, token);
            if(aggIdx >= 0) {
                tokenizer.pop();
                return Operand.ofAggregate(aggIdx);
            }
            throw Error.InvalidCommandException;
        }
        // 常量数字
        if(isNumber(token)) {
            tokenizer.pop();
            Object val = token.contains(".") ? Double.parseDouble(token) : Long.parseLong(token);
            return Operand.ofConstant(val);
        }
        throw Error.InvalidCommandException;
    }

    /**
     * 判断是否是分组列
     */
    private static int getGroupingColumnIndex(Select select, String token) {
        if(select.groupBy == null) return -1;
        for (int i = 0; i < select.groupBy.length; i++) {
            if(select.groupBy[i].equals(token)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 根据别名获取列
     */
    private static String getColumnByAlias(Select select, String alias) {
        if(select.projection == null || select.groupBy == null) return null;
        for (Select.Item item : select.projection) {
            if(item.isAggregate()) continue;
            if(alias.equals(item.alias)) {
                for (String col : select.groupBy) {
                    if(col.equals(item.field)) {
                        return item.field;
                    }
                }
                return null;
            }
        }
        return null;
    }

    private static int getAggregateIndex(Select select, String funcToken, String arg) {
        if(select.aggregates == null) return -1;
        AggregateFunc func = AggregateFunc.from(funcToken);
        for (int i = 0; i < select.aggregates.length; i++) {
            Aggregate agg = select.aggregates[i];
            String aggField = agg.field;
            boolean matchField = (aggField == null && arg == null)
                    || (aggField != null && aggField.equals(arg));
            if(agg.func == func && matchField) {
                return i;
            }
        }
        return -1;
    }
    
    /**
     * 根据别名获取聚合列索引值
     */
    private static int getAggregateByAlias(Select select, String alias) {
        if(select.projection == null) return -1;
        for (int i = 0; i < select.projection.length; i++) {
            Select.Item item = select.projection[i];
            if(item.isAggregate() && alias.equals(item.alias)) {
                // 找到在 aggregates 中的对应下标
                for (int j = 0; j < select.aggregates.length; j++) {
                    if(select.aggregates[j] == item.aggregate) {
                        return j;
                    }
                }
            }
        }
        return -1;
    }

    private static boolean isNumber(String token) {
        if(token == null || token.isEmpty()) return false;
        try {
            Double.parseDouble(token);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
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

    private static String parseAliasIfPresent(Tokenizer tokenizer) throws Exception {
        String next = tokenizer.peek();
        if(eq(next, "as")) {
            tokenizer.pop();
            String alias = tokenizer.peek();
            if(!isName(alias)) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
            return alias;
        }
        if(isName(next) 
            && !eq(next, "from") 
            && !eq(next, "where") 
            && !eq(next, "group") 
            && !eq(next, "having")
            && !",".equals(next)) {
            tokenizer.pop();
            return next;
        }
        return null;
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
        String primaryField = null;

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

            // [FieldName][TypeName][IndexUid][UniqueFlag][PrimaryFlag]
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
                    primaryField = fieldName;
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

            // after a field def, expect ',' or ')' handled by loop
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
        create.primary = primaryField;
        // 当前要求必须指定主键
        if(create.primary == null) {
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

    private static Aggregate parseAggregate(Tokenizer tokenizer) throws Exception {
        String funcToken = tokenizer.peek();
        tokenizer.pop();
        if(!"(".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        String arg = tokenizer.peek();
        boolean star = false;
        if("*".equals(arg)) {
            star = true;
            tokenizer.pop();
        } else {
            if(!isName(arg)) {
                throw Error.InvalidCommandException;
            }
            tokenizer.pop();
        }
        if(!")".equals(tokenizer.peek())) {
            throw Error.InvalidCommandException;
        }
        tokenizer.pop();
        AggregateFunc func;
        try {
            func = AggregateFunc.from(funcToken);
        } catch (Exception e) {
            throw Error.InvalidCommandException;
        }
        if(star && !func.allowStar()) {
            throw Error.InvalidCommandException;
        }
        Aggregate agg = new Aggregate();
        agg.func = func;
        agg.field = star ? null : arg;
        return agg;
    }

    /**
     * 判断name 是否合法
     * @param name 被判断的名称
     */
    private static boolean isName(String name) {
        if(name == null || name.isEmpty()) {
            return false;
        }
        byte[] bytes = name.getBytes();
        byte first = bytes[0];
        // 标识符首字符必须是字母或下划线
        if(!(Tokenizer.isAlpha(first) || first == '_')) {
            return false;
        }
        // 其余字符可包含字母/数字/下划线
        for(int i = 1; i < bytes.length; i++) {
            byte b = bytes[i];
            if(!(Tokenizer.isAlpha(b) || Tokenizer.isDigit(b) || b == '_')) {
                return false;
            }
        }
        return true;
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

    private static boolean isAggregateFunc(String token) {
        if(token == null) return false;
        try {
            AggregateFunc.from(token);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /** 大小写不敏感比较 */
    private static boolean eq(String token, String keyword) {
        return keyword.equalsIgnoreCase(token);
    }

}
