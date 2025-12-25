package com.minisql.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;

import com.google.common.primitives.Bytes;

import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.parser.statement.Where;
import com.minisql.backend.parser.statement.SingleExpression;
import com.minisql.backend.tbm.Field.ParseValueRes;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ParseStringRes;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.ResultSet;
import com.minisql.common.Error;

/**
 * 表（Table）对象，维护表的元信息、字段定义以及增删改查的核心流程。
 *
 * <p>持久化二进制布局：</p>
 * <pre>
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * </pre>
 *
 * <p>说明：</p>
 * <ul>
 *   <li>TableName：变长字符串，Parser.string2Byte 编码</li>
 *   <li>NextTable：8 字节 long，指向下一张表的 UID（链表式组织）</li>
 *   <li>FieldXUid：每个字段在 VM 中的 UID，均为 8 字节 long</li>
 * </ul>
 */
public class Table {
    TableManager tbm;
    long uid;
    String name;
    byte status;
    long nextUid;
    
    /**
     * 表里所有字段（列）的定义列表
     */
    List<Field> fields = new ArrayList<>();

    /**
     * 通过 UID 从持久化存储加载表的元信息。
     *
     * @param tbm 表管理器
     * @param uid 表在 VM 中的唯一标识
     * @return 反序列化后的 Table 实例
     * @throws RuntimeException 读取或解析失败时会触发 Panic.panic 包装后的运行时异常
     */
    public static Table loadTable(TableManager tbm, long uid) {
        byte[] raw = null;
        try {
            // 使用超级事务 SUPER_XID 去加载数据表
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(raw == null) {
            throw new RuntimeException("Load table meta failed: vm.read returned null for uid " + uid);
        }
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 根据 CREATE 语句创建表结构，并将元信息持久化。
     *
     * <p>会根据 {@code create.index} 和 {@code create.unique} 决定字段的索引/唯一性；唯一字段必定建立索引。</p>
     *
     * @param tbm     表管理器
     * @param nextUid 下一张表的 UID（用于形成链表）
     * @param xid     事务 ID
     * @param create  解析后的 CREATE 语句
     * @return 已持久化并带有有效 UID 的 Table 实例
     * @throws Exception VM 写入失败或字段创建失败
     */
    public static Table createTable(TableManager tbm, long nextUid, long xid, Create create) throws Exception {
        Table tb = new Table(tbm, create.tableName, nextUid);
        for(int i = 0; i < create.fieldName.length; i ++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for(int j = 0; j < create.index.length; j ++) {
                if(fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            boolean unique = false;
            if(create.unique != null) {
                for (String uniqueField : create.unique) {
                    if(fieldName.equals(uniqueField)) {
                        unique = true;
                        break;
                    }
                }
            }
            if(unique) {
                indexed = true;
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed, unique));
        }

        return tb.persistSelf(xid);
    }

    /**
     * 通过 UID 构造 Table（延后解析）。
     *
     * @param tbm 表管理器
     * @param uid 表 UID
     */
    public Table(TableManager tbm, long uid) {
        this.tbm = tbm;
        this.uid = uid;
    }

    /**
     * 以名称和 nextUid 构造 Table（用于新建）。
     *
     * @param tbm        表管理器
     * @param tableName  表名
     * @param nextUid    下一张表的 UID
     */
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    /**
     * 解析持久化字节数组，回填本对象的 name、nextUid、fields。
     *
     * @param raw 原始字节数据
     * @return 当前 Table 自身（便于链式调用）
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = ByteUtil.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = ByteUtil.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while(position < raw.length) {
            long uid = ByteUtil.parseLong(Arrays.copyOfRange(raw, position, position + 8));
            position += 8;
            fields.add(Field.loadField(this, uid));
        }
        return this;
    }

    /**
     * 将当前表的元信息序列化并写入 VM，成功后为表分配 UID。
     *
     * @param xid 事务 ID
     * @return 当前 Table 自身（已持久化并具有有效 uid）
     * @throws Exception VM 写入失败
     */
    private Table persistSelf(long xid) throws Exception {
        byte[] nameRaw = ByteUtil.string2Byte(name);
        byte[] nextRaw = ByteUtil.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, ByteUtil.long2Byte(field.uid));
        }
        uid = ((TableManagerImpl)tbm).vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    /**
     * 根据 WHERE 条件删除记录。
     *
     * @param xid    事务 ID
     * @param delete 解析后的 DELETE 语句
     * @return 实际删除的行数
     * @throws Exception VM 读写异常、字段/索引异常
     */
    public int delete(long xid, Delete delete) throws Exception {
        List<Long> uids = applyWhere(xid, delete.where);
        int count = 0;
        for (Long uid : uids) {
            if(((TableManagerImpl)tbm).vm.delete(xid, uid)) {
                count ++;
            }
        }
        return count;
    }

    /**
     * 按 WHERE 条件更新指定字段，并维护二级索引与唯一约束。
     *
     * <p>实现：读原行 → 解析为键值对 → 覆盖目标字段 → 校验唯一约束 → 物理删除原行 → 插入新行 → 回填索引。</p>
     *
     * @param xid    事务 ID
     * @param update 解析后的 UPDATE 语句
     * @return 受影响的行数
     * @throws Exception 字段不存在、未建索引、唯一约束冲突或 VM 异常
     */
    public int update(long xid, Update update) throws Exception {
        List<Long> uids = applyWhere(xid, update.where);
        Field fd = null;
        for (Field f : fields) {
            if(f.fieldName.equals(update.fieldName)) {
                fd = f;
                break;
            }
        }
        if(fd == null) {
            throw Error.FieldNotFoundException;
        }
        Object value = fd.string2Value(update.value);
        int count = 0;
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            Map<String, Object> rowData = parseEntry(raw);
            rowData.put(fd.fieldName, value);

            validateUniqueConstraints(rowData, uid);
            // 删除旧记录
            ((TableManagerImpl)tbm).vm.delete(xid, uid);

            raw = RowData2Raw(rowData);
            // 插入新记录
            long uuid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
            
            count ++;

            for (Field field : fields) {
                if(field.isIndexed()) {
                    field.insert(rowData.get(field.fieldName), uuid);
                }
            }
        }
        return count;
    }

    /**
     * 执行 SELECT 查询并返回结构化结果。
     *
     * @param xid  事务 ID
     * @param read 解析后的 SELECT 语句
     * @return 结构化结果集
     * @throws Exception 字段不存在、索引异常或 VM 异常
     */
    public ResultSet read(long xid, Select read) throws Exception {
        List<Long> uids = applyWhere(xid, read.where);
        List<Field> targetFields = resolveSelectFields(read.fields);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            rows.add(parseEntry(raw));
        }
        return formatTable(targetFields, rows);
    }

    /**
     * 插入一条记录，并维护索引与唯一约束。
     *
     * @param xid    事务 ID
     * @param insert 解析后的 INSERT 语句
     * @throws Exception 值个数不匹配、唯一约束冲突、VM 异常
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> rowData = string2RowData(insert.values);
        validateUniqueConstraints(rowData, null);
        byte[] raw = RowData2Raw(rowData);
        long uid = ((TableManagerImpl)tbm).vm.insert(xid, raw);
        for (Field field : fields) {
            if(field.isIndexed()) {
                field.insert(rowData.get(field.fieldName), uid);
            }
        }
    }

    /**
     * 校验所有唯一字段，避免与其他记录冲突。
     *
     * @param entry   待校验的记录（字段名 → 值）
     * @param selfUid 本记录原 UID；插入场景传 {@code null}，更新场景用于跳过自身
     * @throws Exception 唯一约束冲突或索引异常
     */
    private void validateUniqueConstraints(Map<String, Object> entry, Long selfUid) throws Exception {
        for (Field field : fields) {
            if(field.isUnique()) {
                field.ensureUnique(entry.get(field.fieldName), selfUid);
            }
        }
    }

    /**
     * 将 INSERT 的字符串 VALUES 转换为字段值映射。
     *
     * @param values 与字段一一对应的字符串数组
     * @return 字段名 → 具体值 的映射
     * @throws Exception 个数不匹配或类型解析失败
     */
    private Map<String, Object> string2RowData(String[] values) throws Exception {
        if(values.length != fields.size()) {
            throw Error.InvalidValuesException;
        }
        // 字段名 → 具体值
        Map<String, Object> rowData = new HashMap<>();
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            Object v = f.string2Value(values[i]);
            rowData.put(f.fieldName, v);
        }
        return rowData;
    }

    /**
     * 解析 WHERE 条件，结合索引情况做优化：
     *
     * 索引情况   AND                              OR
     * a✔ b✔     两索引各自查，取交集              两索引各自查，取并集
     * a✔ b✘     用 a 索引做候选，再过滤 b         用 a 索引查 a，再全表扫描匹配 b，最后并集
     * a✘ b✔     用 b 索引做候选，再过滤 a         用 b 索引查 b，再全表扫描匹配 a，最后并集
     * a✘ b✘     全表扫描 + matchWhere            全表扫描 + matchWhere
     *
     * 同时保留：
     *  - 单表达式优化（一个字段）
     *  - 同字段多表达式的 calWhere 优化
     *  - 出现 "!=" 统一退回全表扫描保证正确性
     */
    private List<Long> applyWhere(long xid, Where where) throws Exception {
        // 1. 没有 WHERE：全表扫描
        if (where == null) {
            return getUidsByWhere(xid, null);
        }

        // 2. 出现 "!="：退回全表扫描 + matchWhere
        boolean hasNotEqual =
                (where.singleExp1 != null && where.singleExp1.op == CompareOperator.NE)
             || (where.singleExp2 != null && where.singleExp2.op == CompareOperator.NE);
        if (hasNotEqual) {
            return getUidsByWhere(xid, where);
        }

        // 3. 只有一个表达式：单字段优化
        LogicOperator logicOperator = LogicOperator.from(where.logicOp);

        if (where.singleExp2 == null || logicOperator == LogicOperator.NONE) {
            SingleExpression exp = where.singleExp1;
            Field f = getFieldByName(exp.field);
            if (f == null) throw Error.FieldNotFoundException;

            if (f.isIndexed()) {
                Range r = f.calExp(exp);
                List<Long> uids = f.search(r.getLeft(), r.getRight());
                Set<Long> uidSet = new LinkedHashSet<>();
                if (uids != null) uidSet.addAll(uids);
                return filterUidsByWhere(xid, uidSet, where);
            }

            return getUidsByWhere(xid, where);
        }

        // 4. 有两个表达式
        SingleExpression exp1 = where.singleExp1;
        SingleExpression exp2 = where.singleExp2;
        Field f1 = getFieldByName(exp1.field);
        Field f2 = getFieldByName(exp2.field);
        if (f1 == null || f2 == null) {
            throw Error.FieldNotFoundException;
        }

        // 4.1 同字段且有索引，继续用 calWhere 做单字段范围优化
        if (exp1.field.equals(exp2.field) && f1.isIndexed()) {
            List<Range> ranges = calWhere(f1, where);
            if (ranges.isEmpty()) {
                return new ArrayList<>();
            }
            Set<Long> uidSet = new LinkedHashSet<>();
            for (Range r : ranges) {
                List<Long> uids = f1.search(r.getLeft(), r.getRight());
                if (uids != null) uidSet.addAll(uids);
            }
            return filterUidsByWhere(xid, uidSet, where);
        }

        // 4.2 不同字段 a, b 的情况
        boolean aIndexed = f1.isIndexed();
        boolean bIndexed = f2.isIndexed();

        // AND 逻辑
        if (logicOperator == LogicOperator.AND) {
            if (!aIndexed && !bIndexed) {
                return getUidsByWhere(xid, where);
            }
            // a, b 都有索引
            if (aIndexed && bIndexed) {
                Set<Long> setA = getUidsByIndexAndExp(xid, f1, exp1);
                Set<Long> setB = getUidsByIndexAndExp(xid, f2, exp2);
                setA.retainAll(setB); // A与B取交集
                return new ArrayList<>(setA);
            }
            // a 索引，b 无索引
            if (aIndexed) {
                Set<Long> setA = getUidsByIndexAndExp(xid, f1, exp1);
                return filterUidsByWhere(xid, setA, where);
            }
            // b 索引，a 无索引
            else {
                Set<Long> setB = getUidsByIndexAndExp(xid, f2, exp2);
                return filterUidsByWhere(xid, setB, where);
            }
        }

        // OR 逻辑
        if (logicOperator == LogicOperator.OR) {
            // a, b 索引
            if (!aIndexed && !bIndexed) {
                return getUidsByWhere(xid, where);
            }
            Set<Long> result = new LinkedHashSet<>();
            // a 索引, b 无索引: 用 a 索引做候选，再全表扫描匹配 b
            if (aIndexed) {
                result.addAll(getUidsByIndexAndExp(xid, f1, exp1));
            } else {
                result.addAll(getUidsByExp(xid, exp1));
            }
            // b 索引, a 无索引: 用 b 索引做候选，再全表扫描匹配 a
            if (bIndexed) {
                result.addAll(getUidsByIndexAndExp(xid, f2, exp2));
            } else {
                result.addAll(getUidsByExp(xid, exp2));
            }
            return new ArrayList<>(result);
        }

        throw Error.InvalidLogOpException;
    }

    /**
     * 全表扫描：通过某个索引字段扫描所有 UID，再在内存中用 where 条件过滤。
     * 如果 where == null，则等价于“返回整张表所有 UID”。
     */
    private List<Long> getUidsByWhere(long xid, Where where) throws Exception {
        List<Long> all = getAllUids();

        if (where == null) return all;

        List<Long> matched = new ArrayList<>();
        for (Long uid : all) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> row = parseEntry(raw);
            if (matchWhere(row, where)) {
                matched.add(uid);
            }
        }
        return matched;
    }

    /**
     * 利用某个索引字段做全范围搜索，相当于拿到整张表的所有 UID。
     */
    private List<Long> getAllUids() throws Exception {
        Field scanIndex = pickScanIndex();
        return scanIndex.search(Long.MIN_VALUE, Long.MAX_VALUE);
    }



    /**
     * 将 WHERE 表达式转换为一个或多个数值区间，用于索引定位。
     * 这里只处理没有 "!=" 的情况，且假定两个表达式使用同一个字段。
     */
    private List<Range> calWhere(Field fd, Where where) throws Exception {
        List<Range> ranges = new ArrayList<>();
        LogicOperator op = LogicOperator.from(where.logicOp);
        switch (op) {
            case NONE: {
                Range r = fd.calExp(where.singleExp1);
                ranges.add(r);
                break;
            }
            case OR: {
                Range r1 = fd.calExp(where.singleExp1);
                Range r2 = fd.calExp(where.singleExp2);
                ranges.add(r1);
                ranges.add(r2);
                break;
            }
            case AND: {
                Range r1 = fd.calExp(where.singleExp1);
                Range r2 = fd.calExp(where.singleExp2);
                long left = Math.max(r1.getLeft(), r2.getLeft());
                long right = Math.min(r1.getRight(), r2.getRight());
                if (left <= right) {
                    ranges.add(new Range(left, right));
                }
                break;
            }
            default:
                throw Error.InvalidLogOpException;
        }

        return ranges;
    }

    private Field pickScanIndex() throws Exception {
        for (Field field : fields) {
            if(field.isUnique() && field.isIndexed()) {
                return field;
            }
        }
        for (Field field : fields) {
            if(field.isIndexed()) {
                return field;
            }
        }
        throw Error.FieldNotIndexedException;
    }

    private boolean matchWhere(Map<String, Object> row, Where where) throws Exception {
        if(where == null) {
            return true;
        }
        LogicOperator op = LogicOperator.from(where.logicOp);
        switch(op) {
            case NONE:
                return matchExp(row, where.singleExp1);
            case AND:
                return matchExp(row, where.singleExp1) && matchExp(row, where.singleExp2);
            case OR:
                return matchExp(row, where.singleExp1) || matchExp(row, where.singleExp2);
            default:
                throw Error.InvalidLogOpException;
        }
    }

    private boolean matchExp(Map<String, Object> row, SingleExpression exp) throws Exception {
        // 1. 找到对应字段定义
        Field target = null;
        for (Field field : fields) {
            if(field.fieldName.equals(exp.field)) {
                target = field;
                break;
            }
        }
        if(target == null) {
            throw Error.FieldNotFoundException;
        }
        // 2. 从 row 中拿到该字段的当前实际值
        Object curVal = row.get(target.fieldName);
        if(curVal == null) return false;

        FieldType type = FieldType.from(target.fieldType);
        // 3. 把表达式中的右值字符串转成对应类型
        Object expectVal = type.parse(exp.value);

        // 4. 用 FieldType 的策略比较两边
        int cmp = type.compare(curVal, expectVal);

        // 5. 用 CompareOperator 的策略判断是否满足运算符
        CompareOperator op = exp.op;
        return op.match(cmp);
    }

    private Field getFieldByName(String fieldName) {
        for (Field field : fields) {
            if (field.fieldName.equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    /**
     * 使用指定字段的索引，根据单个表达式做范围搜索，并按事务可见性和条件匹配过滤。
     */
    private Set<Long> getUidsByIndexAndExp(long xid, Field f, SingleExpression exp) throws Exception {
        Set<Long> res = new LinkedHashSet<>();
        Range r = f.calExp(exp);
        List<Long> uids = f.search(r.getLeft(), r.getRight());
        if (uids == null || uids.isEmpty()) return res;

        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> row = parseEntry(raw);
            if (matchExp(row, exp)) {
                res.add(uid);
            }
        }
        return res;
    }

    /**
     * 全表扫描单个表达式匹配（用于一侧无索引的 OR）。
     */
    private Set<Long> getUidsByExp(long xid, SingleExpression exp) throws Exception {
        Set<Long> res = new LinkedHashSet<>();
        List<Long> all = getAllUids();
        for (Long uid : all) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> row = parseEntry(raw);
            if (matchExp(row, exp)) {
                res.add(uid);
            }
        }
        return res;
    }

    /**
     * 对一批 UID 做 vm.read + matchWhere 过滤，统一处理事务可见性和复杂逻辑。
     */
    private List<Long> filterUidsByWhere(long xid, Set<Long> uidSet, Where where) throws Exception {
        List<Long> res = new ArrayList<>();
        for (Long uid : uidSet) {
            byte[] raw = ((TableManagerImpl) tbm).vm.read(xid, uid);
            if (raw == null) continue;
            Map<String, Object> row = parseEntry(raw);
            if (matchWhere(row, where)) {
                res.add(uid);
            }
        }
        return res;
    }

    /**
     * 解析 SELECT 列清单，返回目标字段列表。
     *
     * <p>支持：空/星号（全部字段）/指定字段名列表。</p>
     *
     * @param selectFields SELECT 中的列名数组；为 null、空数组或仅含 "*" 表示全部字段
     * @return 目标字段列表（按输入顺序）
     * @throws Exception 字段不存在
     */
    private List<Field> resolveSelectFields(String[] selectFields) throws Exception {
        if(selectFields == null || selectFields.length == 0) {
            return fields;
        }
        if(selectFields.length == 1 && "*".equals(selectFields[0])) {
            return fields;
        }
        List<Field> targetFields = new ArrayList<>();
        for (String fieldName : selectFields) {
            Field matched = null;
            for (Field field : fields) {
                if(field.fieldName.equals(fieldName)) {
                    matched = field;
                    break;
                }
            }
            if(matched == null) {
                throw Error.FieldNotFoundException;
            }
            targetFields.add(matched);
        }
        return targetFields;
    }

    /**
     * To ResultSet
     * 将查询结果转为结构化数据。
     * 
     * @param targetFields 需要输出的字段顺序
     * @param rows         数据行（字段名 → 值）
     * @return 结构化结果集
     */
    private ResultSet formatTable(List<Field> targetFields, List<Map<String, Object>> rows) {
        int columnCount = targetFields.size();
        List<List<String>> dataRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            List<String> rowValues = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Field field = targetFields.get(i);
                String value = field.printValue(row.get(field.fieldName));
                if(value == null) value = "NULL";
                rowValues.add(value);
            }
            dataRows.add(rowValues);
        }

        List<String> headers = new ArrayList<>();
        for (Field field : targetFields) {
            headers.add(field.fieldName);
        }
        return new ResultSet(headers, dataRows);
    }

    /**
     * 将一行二进制数据解析为（字段名 → 值）的映射。
     *
     * @param raw 行原始字节
     * @return 字段值映射
     */
    private Map<String, Object> parseEntry(byte[] raw) {
        int pos = 0;
        Map<String, Object> entry = new HashMap<>();
        for (Field field : fields) {
            ParseValueRes r = field.parserValue(Arrays.copyOfRange(raw, pos, raw.length));
            entry.put(field.fieldName, r.v);
            pos += r.shift;
        }
        return entry;
    }

    /**
     * 将（字段名 → 值）的映射编码为行二进制。
     *
     * @param entry 字段值映射
     * @return 行原始字节
     */
    private byte[] RowData2Raw(Map<String, Object> entry) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.value2Raw(entry.get(field.fieldName)));
        }
        return raw;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for(Field field : fields) {
            sb.append(field.toString());
            if(field == fields.get(fields.size()-1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
