package top.guoziyang.mydb.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.parser.statement.Create;
import top.guoziyang.mydb.backend.parser.statement.Delete;
import top.guoziyang.mydb.backend.parser.statement.Insert;
import top.guoziyang.mydb.backend.parser.statement.Select;
import top.guoziyang.mydb.backend.parser.statement.Update;
import top.guoziyang.mydb.backend.parser.statement.Where;
import top.guoziyang.mydb.backend.tbm.Field.ParseValueRes;
import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.ParseStringRes;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.backend.utils.format.TextTableFormatter;
import top.guoziyang.mydb.common.Error;

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
            raw = ((TableManagerImpl)tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
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
        ParseStringRes res = Parser.parseString(raw);
        name = res.str;
        position += res.next;
        nextUid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
        position += 8;

        while(position < raw.length) {
            long uid = Parser.parseLong(Arrays.copyOfRange(raw, position, position+8));
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
        byte[] nameRaw = Parser.string2Byte(name);
        byte[] nextRaw = Parser.long2Byte(nextUid);
        byte[] fieldRaw = new byte[0];
        for(Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, Parser.long2Byte(field.uid));
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
        List<Long> uids = parseWhere(delete.where);
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
        List<Long> uids = parseWhere(update.where);
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

            ((TableManagerImpl)tbm).vm.delete(xid, uid);
            raw = RowData2Raw(rowData);
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
     * 执行 SELECT 查询并以 ASCII 表格返回结果。
     *
     * @param xid  事务 ID
     * @param read 解析后的 SELECT 语句
     * @return 渲染后的文本表格
     * @throws Exception 字段不存在、索引异常或 VM 异常
     */
    public String read(long xid, Select read) throws Exception {
        List<Long> uids = parseWhere(read.where);
        List<Field> targetFields = resolveSelectFields(read.fields);
        List<Map<String, Object>> rows = new ArrayList<>();
        for (Long uid : uids) {
            byte[] raw = ((TableManagerImpl)tbm).vm.read(xid, uid);
            if(raw == null) continue;
            rows.add(parseEntry(raw));
        }
        SelectStats.setRowCount(rows.size());
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
     * 解析 WHERE 条件，通过索引检索匹配行的 UID 集合。
     *
     * <p>若 WHERE 为空，则选择第一个已建索引字段的全范围扫描；否则依据表达式求解范围并查询。</p>
     *
     * @param where WHERE 子句（可为 null）
     * @return 匹配记录的 UID 列表
     * @throws Exception 字段未建索引、字段不存在或索引异常
     */
    private List<Long> parseWhere(Where where) throws Exception {
        long l0=0, r0=0, l1=0, r1=0;
        boolean single = false;
        Field fd = null;
        if(where == null) {
            for (Field field : fields) {
                if(field.isIndexed()) {
                    fd = field;
                    break;
                }
            }
            l0 = 0;
            r0 = Long.MAX_VALUE;
            single = true;
        } else {
            for (Field field : fields) {
                if(field.fieldName.equals(where.singleExp1.field)) {
                    if(!field.isIndexed()) {
                        throw Error.FieldNotIndexedException;
                    }
                    fd = field;
                    break;
                }
            }
            if(fd == null) {
                throw Error.FieldNotFoundException;
            }
            CalWhereRes res = calWhere(fd, where);
            l0 = res.l0; r0 = res.r0;
            l1 = res.l1; r1 = res.r1;
            single = res.single;
        }
        List<Long> uids = fd.search(l0, r0);
        if(!single) {
            List<Long> tmp = fd.search(l1, r1);
            uids.addAll(tmp);
        }
        return uids;
    }

    /**
     * WHERE 计算结果容器。
     */
    class CalWhereRes {
        long l0, r0, l1, r1;
        boolean single;
    }

    /**
     * 将 WHERE 表达式转换为数值范围，用于索引定位。
     *
     * @param fd    参与过滤的字段（必须已建索引）
     * @param where WHERE 子句
     * @return 计算得到的范围结果（单区间或双区间）
     * @throws Exception 非法逻辑操作符或表达式解析失败
     */
    private CalWhereRes calWhere(Field fd, Where where) throws Exception {
        CalWhereRes res = new CalWhereRes();
        switch(where.logicOp) {
            case "":
                res.single = true;
                FieldCalRes r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                break;
            case "or":
                res.single = false;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                break;
            case "and":
                res.single = true;
                r = fd.calExp(where.singleExp1);
                res.l0 = r.left; res.r0 = r.right;
                r = fd.calExp(where.singleExp2);
                res.l1 = r.left; res.r1 = r.right;
                if(res.l1 > res.l0) res.l0 = res.l1;
                if(res.r1 < res.r0) res.r0 = res.r1;
                break;
            default:
                throw Error.InvalidLogOpException;
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
     * 将查询结果渲染为统一的文本表格。
     *
     * @param targetFields 需要输出的字段顺序
     * @param rows         数据行（字段名 → 值）
     * @return 文本表格（ASCII）
     */
    private String formatTable(List<Field> targetFields, List<Map<String, Object>> rows) {
        int columnCount = targetFields.size();
        int[] widths = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            widths[i] = targetFields.get(i).fieldName.length();
        }
        List<List<String>> dataRows = new ArrayList<>();
        for (Map<String, Object> row : rows) {
            List<String> rowValues = new ArrayList<>();
            for (int i = 0; i < columnCount; i++) {
                Field field = targetFields.get(i);
                String value = field.printValue(row.get(field.fieldName));
                if(value == null) value = "NULL";
                rowValues.add(value);
                if(value.length() > widths[i]) {
                    widths[i] = value.length();
                }
            }
            dataRows.add(rowValues);
        }

        List<String> headers = new ArrayList<>();
        for (Field field : targetFields) {
            headers.add(field.fieldName);
        }
        return TextTableFormatter.format(headers, dataRows);
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
