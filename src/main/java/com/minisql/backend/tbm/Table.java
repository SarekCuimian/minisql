package com.minisql.backend.tbm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.LinkedHashSet;

import com.google.common.primitives.Bytes;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.parser.statement.Create;
import com.minisql.backend.parser.statement.Delete;
import com.minisql.backend.parser.statement.Insert;
import com.minisql.backend.parser.statement.Select;
import com.minisql.backend.parser.statement.Update;
import com.minisql.backend.parser.statement.Where;
import com.minisql.backend.parser.statement.SingleExpression;
import com.minisql.backend.parser.statement.Aggregate;
import com.minisql.backend.parser.statement.condition.Condition;
import com.minisql.backend.parser.statement.condition.BinaryCondition;
import com.minisql.backend.parser.statement.condition.PredicateCondition;
import com.minisql.backend.parser.statement.condition.Operand;
import com.minisql.backend.aggregator.AggregateContext;
import com.minisql.backend.parser.statement.operator.CompareOperator;
import com.minisql.backend.parser.statement.operator.LogicOperator;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.backend.utils.ParsedValue;
import com.minisql.backend.vm.VersionManager;
import com.minisql.common.ResultSet;
import com.minisql.common.Error;

/**
 * 表（Table）对象，维护表的元信息、字段定义以及增删改查的核心流程。
 *
 * <p>
 * 持久化二进制布局：
 * </p>
 * 
 * <pre>
 * [TableName][NextTable]
 * [Field1Uid][Field2Uid]...[FieldNUid]
 * </pre>
 *
 * <p>
 * 说明：
 * </p>
 * <ul>
 * <li>TableName：变长字符串，Parser.stringToByte 编码</li>
 * <li>NextTable：8 字节 long，指向下一张表的 UID（链表式组织）</li>
 * <li>FieldXUid：每个字段在 VM 中的 UID，均为 8 字节 long</li>
 * </ul>
 */
public class Table {
    long uid;
    String name;
    long nextUid;
    /**
     * 表里所有字段（列）的定义列表
     */
    List<Field> fields = new ArrayList<>();

    TableManager tbm;
    VersionManager vm;
    DataManager dm;

    // =========================================================
    // 静态工厂 / 加载方法
    // =========================================================

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
            // raw = vm.read(TransactionManagerImpl.SUPER_XID, uid);
            raw = tbm.getVersionManager().read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.of(e);
        }
        if (raw == null) {
            throw new RuntimeException("Load table meta failed: vm.read returned null for uid " + uid);
        }
        Table tb = new Table(tbm, uid);
        return tb.parseSelf(raw);
    }

    /**
     * 根据 CREATE 语句创建表结构，并将元信息持久化。
     *
     * <p>
     * 会根据 {@code create.fsm} 和 {@code create.unique} 决定字段的索引/唯一性；唯一字段必定建立索引。
     * </p>
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

        for (int i = 0; i < create.fieldName.length; i++) {
            String fieldName = create.fieldName[i];
            String fieldType = create.fieldType[i];
            boolean indexed = false;
            for (int j = 0; j < create.index.length; j++) {
                if (fieldName.equals(create.index[j])) {
                    indexed = true;
                    break;
                }
            }
            boolean unique = false;
            if (create.unique != null) {
                for (String uniqueField : create.unique) {
                    if (fieldName.equals(uniqueField)) {
                        unique = true;
                        break;
                    }
                }
            }
            boolean primary = fieldName.equals(create.primary);
            if (primary) {
                unique = true;
                indexed = true;
            } else if (unique) {
                indexed = true;
            }
            tb.fields.add(Field.createField(tb, xid, fieldName, fieldType, indexed, unique, primary));
        }

        return tb.persistSelf(xid);
    }

    // =========================================================
    // 构造方法
    // =========================================================

    /**
     * 通过 UID 构造 Table（延后解析）。
     *
     * @param tbm 表管理器
     * @param uid 表 UID
     */
    public Table(TableManager tbm, long uid) {
        this.uid = uid;
        this.tbm = tbm;
        this.vm = tbm.getVersionManager();
        this.dm = tbm.getDataManager();
    }

    /**
     * 以名称和 nextUid 构造 Table（用于新建）。
     *
     * @param tbm       表管理器
     * @param tableName 表名
     * @param nextUid   下一张表的 UID
     */
    public Table(TableManager tbm, String tableName, long nextUid) {
        this.tbm = tbm;
        this.name = tableName;
        this.nextUid = nextUid;
    }

    // =========================================================
    // 元信息解析 / 持久化
    // =========================================================

    /**
     * 解析持久化字节数组，回填本对象的 name、nextUid、fields。
     *
     * @param raw 原始字节数据
     * @return 当前 Table 自身（便于链式调用）
     */
    private Table parseSelf(byte[] raw) {
        int position = 0;
        ParsedValue parsed = ByteUtil.parseString(raw);
        name = (String) parsed.value;
        position += parsed.size;
        nextUid = ByteUtil.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        position += 8;

        while (position < raw.length) {
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
        byte[] nameRaw = ByteUtil.stringToByte(name);
        byte[] nextRaw = ByteUtil.longToByte(nextUid);
        byte[] fieldRaw = new byte[0];
        for (Field field : fields) {
            fieldRaw = Bytes.concat(fieldRaw, ByteUtil.longToByte(field.uid));
        }
        uid = vm.insert(xid, Bytes.concat(nameRaw, nextRaw, fieldRaw));
        return this;
    }

    // =========================================================
    // 公共 API：INSERT / SELECT / UPDATE / DELETE
    // =========================================================

    /**
     * 插入一条记录，并维护索引与唯一约束。
     *
     * @param xid    事务 ID
     * @param insert 解析后的 INSERT 语句
     * @throws Exception 值个数不匹配、唯一约束冲突、VM 异常
     */
    public void insert(long xid, Insert insert) throws Exception {
        Map<String, Object> valueMap = getValueMap(insert.columns, insert.values);
        validateUniqueConstraints(xid, valueMap, null);
        byte[] raw = serializeValueMap(valueMap);
        long uid = vm.insert(xid, raw);
        for (Field field : fields) {
            if (field.isIndexed()) {
                field.insert(valueMap.get(field.fieldName), uid);
            }
        }
    }

    /**
     * 执行 SELECT 查询并返回结构化结果。
     *
     * @param xid    事务 ID
     * @param select 解析后的 SELECT 语句
     * @return 结构化结果集
     * @throws Exception 字段不存在、索引异常或 VM 异常
     */
    public ResultSet read(long xid, Select select) throws Exception {
        boolean hasAggregate = select.aggregates != null && select.aggregates.length > 0;
        boolean hasGroup = select.groupBy != null && select.groupBy.length > 0;
        // 有分组，先验证分组列是否合法
        if (hasGroup) {
            validateGroupBy(select);
        } else if (hasAggregate && select.fields != null && select.fields.length > 0) {
            // 无分组时，禁止聚合与普通列混用
            throw Error.InvalidCommandException;
        }
        // 应用 WHERE 条件 确定 UID 范围
        List<Long> uids = applyWhere(xid, select.where);
        // 有聚合函数
        if (hasAggregate) {
            // 有聚合函数且有分组
            if (hasGroup) {
                return aggregateByGroup(xid, select, uids);
            }
            // 有聚合函数，无分组
            // 用带别名/投影顺序的封装
            return aggregate(xid, select, uids);
        } else {
            if (hasGroup) {
                // 无聚合但有分组，做去重
                return distinctByGroup(xid, select, uids);
            } else {
                // 无聚合函数，普通查询
                return toResultSet(xid, select, uids);
            }
        }
    }

    /**
     * 按 WHERE 条件更新指定字段，并维护二级索引与唯一约束。
     *
     * <p>
     * 实现：读原行 → 解析为键值对 → 覆盖目标字段 → 校验唯一约束 → 物理删除原行 → 插入新行 → 回填索引。
     * </p>
     *
     * @param xid    事务 ID
     * @param update 解析后的 UPDATE 语句
     * @return 受影响的行数
     * @throws Exception 字段不存在、未建索引、唯一约束冲突或 VM 异常
     */
    public int update(long xid, Update update) throws Exception {
        // 1. 根据 where 条件找出候选记录版本 uid（可能不是最新版本）
        List<Long> uids = applyWhere(xid, update.where);

        // 2. 找到要更新的字段 fd，以及主键字段 pkField
        Field fd = null;
        Field pkField = null;
        for (Field f : fields) {
            if (f.fieldName.equals(update.fieldName))
                fd = f;
            if (f.isPrimary())
                pkField = f;
        }
        if (fd == null)
            throw Error.FieldNotFoundException;
        if (pkField == null)
            throw Error.InvalidCommandException;
        if (fd.isPrimary())
            throw Error.PrimaryKeyNotUpdatableException;

        // 3. 把字符串形式的新值转换成目标字段的类型
        Object value = fd.stringToValue(update.value);

        int count = 0;
        final int MAX_RETRY = 3;

        // 预读：保存候选 uid 对应的主键值，用于“等待后旧版本不可见(raw==null)”时重定位最新版本
        // 主键不可更新，因此同一逻辑行的不同版本主键值一致。
        Map<Long, Object> uidPrimaryKeyMap = new HashMap<>();
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> row = parseValueMap(raw);
            Object pkVal = row.get(pkField.fieldName);
            if (pkVal != null) {
                uidPrimaryKeyMap.put(uid, pkVal);
            }
        }

        // 4. 对每个候选 uid 逐条更新
        for (Long uid : uids) {
            // curUid：当前准备操作的版本 uid（可能会被切换为最新版本）
            Long curUid = uid;
            Object primaryKeyVal = uidPrimaryKeyMap.get(uid);

            // 5. 重试循环：处理“读不到/版本不是最新/并发产生新版本”等情况
            int retry = 0;
            while (retry < MAX_RETRY && curUid != null) {
                retry++;
                // readForUpdate：读取并加锁
                // 成功：返回该版本 raw
                // 失败(raw==null)：该 uid 在当前事务 xid 视角不可见（可能被删除/被新版本覆盖/不可见）
                byte[] raw = vm.readForUpdate(xid, curUid);

                // 5.1 如果当前 uid 已不可见：尝试用主键值重定位最新版本并重试
                if (raw == null) {
                    if (primaryKeyVal != null) {
                        Long latestUid = getLatestUidByPrimaryKey(xid, pkField, primaryKeyVal);
                        if (latestUid != null && !latestUid.equals(curUid)) {
                            curUid = latestUid;
                            continue;
                        }
                    }
                    break;
                }
                // 5.2 能读到 raw：解析记录得到字段值映射
                Map<String, Object> row = parseValueMap(raw);
                // 并发下版本切换后，可能已不满足 WHERE，保持语义，仅更新执行时满足 WHERE 的行
                if (update.where != null && !matchWhere(row, update.where)) {
                    vm.getLockManager().release(xid, curUid);
                    break;
                }

                // 5.3) readForUpdate 能返回 raw，表示：
                // - 当前 xid 已获得该 uid 的行锁（必要时等待）
                // - 且该版本对当前事务可见
                // 后续直接基于此版本做“删旧插新”，避免额外的二次校准逻辑。
                row.put(fd.fieldName, value);
                // 校验所有 unique 字段不与其他记录冲突，更新场景传入 selfUid 用于跳过被更新的数据自身值
                validateUniqueConstraints(xid, row, curUid);
                // 删除旧版本
                vm.delete(xid, curUid);
                // 序列化新版本并插入，得到新 uid
                raw = serializeValueMap(row);
                long uuid = vm.insert(xid, raw);
                count++;
                // 更新索引：把新版本写入所有 indexed 字段的索引结构
                for (Field f : fields) {
                    if (f.isIndexed()) {
                        f.insert(row.get(f.fieldName), uuid);
                    }
                }
                break;
            }
        }
        return count;
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
            if (vm.delete(xid, uid)) {
                count++;
            }
        }
        return count;
    }

    // =========================================================
    // 唯一约束 / INSERT & UPDATE 辅助
    // =========================================================

    /**
     * 校验所有唯一字段，避免与其他记录冲突。
     *
     * @param valueMap 待校验的记录（字段名 → 值）
     * @param selfUid  本记录原 UID；插入场景传 {@code null}，更新场景用于跳过自身
     * @throws Exception 唯一约束冲突或索引异常
     */
    private void validateUniqueConstraints(long xid, Map<String, Object> valueMap, Long selfUid) throws Exception {
        for (Field field : fields) {
            if (field.isUnique()) {
                field.ensureUnique(xid, valueMap.get(field.fieldName), selfUid);
            }
        }
    }

    /**
     * 根据主键值查找当前事务可见的最新 uid。
     */
    private Long getLatestUidByPrimaryKey(long xid, Field pkField, Object pkValue) throws Exception {
        long key = pkField.toKey(pkValue);
        List<Long> uids = pkField.search(key, key);
        if (uids == null)
            return null;
        Long visibleUid = null;
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            if (visibleUid != null) {
                // 同一主键在同一事务视角下出现多条可见版本，属于数据污染/严重一致性问题，必须 fail-fast。
                throw Error.MultipleVisibleVersionsException;
            }
            visibleUid = uid;
        }
        return visibleUid;
    }

    /**
     * 将 INSERT 的字符串 VALUES 转换为字段值映射。
     *
     * @param columns 指定的列名；为 null 时按表定义顺序
     * @param values  与字段一一对应的字符串数组
     * @return 字段名 → 具体值 的映射
     * @throws Exception 个数不匹配或类型解析失败
     */
    private Map<String, Object> getValueMap(String[] columns, String[] values) throws Exception {
        Map<String, Object> valueMap = new HashMap<>();
        // 解析列名
        if (columns == null || columns.length == 0) {
            if (values.length != fields.size()) {
                throw Error.InvalidValuesException;
            }
            for (int i = 0; i < fields.size(); i++) {
                Field f = fields.get(i);
                Object v = f.stringToValue(values[i]);
                valueMap.put(f.fieldName, v);
            }
            return valueMap;
        }
        // 列数匹配
        if (values.length != columns.length) {
            throw Error.InvalidValuesException;
        }
        // 值匹配列名
        for (int i = 0; i < columns.length; i++) {
            String col = columns[i];
            Field matched = getFieldByName(col);
            if (matched == null) {
                throw Error.FieldNotFoundException;
            }
            if (valueMap.containsKey(col)) {
                throw Error.InvalidValuesException;
            }
            valueMap.put(col, matched.stringToValue(values[i]));
        }
        // 未提供的列：主键缺失报错，其他填默认值（0 / 0L / ""）v
        for (Field field : fields) {
            if (!valueMap.containsKey(field.fieldName)) {
                if (field.isPrimary()) {
                    throw Error.PrimaryKeyMissingException;
                }
                valueMap.put(field.fieldName, field.fieldType.defaultValue());
            }
        }

        return valueMap;
    }

    /**
     * 校验 GROUP BY
     * 列必须存在 && SELECT 里的非聚合列必须全部出现在 GROUP BY 里
     */
    private void validateGroupBy(Select select) throws Exception {
        if (select.groupBy == null || select.groupBy.length == 0)
            return;
        // 分组列必须存在
        for (String column : select.groupBy) {
            if (getFieldByName(column) == null) {
                throw Error.FieldNotFoundException;
            }
        }
        // 如果指定了非聚合列，必须是 group by 列
        if (select.fields != null) {
            for (String column : select.fields) {
                if (getFieldByName(column) == null) {
                    throw Error.FieldNotFoundException;
                }
                boolean inGroup = false;
                for (String g : select.groupBy) {
                    if (g.equals(column)) {
                        inGroup = true;
                        break;
                    }
                }
                if (!inGroup) {
                    throw Error.InvalidCommandException;
                }
            }
        }
    }

    /**
     * 无分组的全表聚合（SELECT 只有聚合函数）。
     */
    private ResultSet aggregate(long xid, Select select, List<Long> uids) throws Exception {
        AggregateContext aggCtx = AggregateContext.of(fields, select.aggregates);
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            // 聚合器容器统一接收row
            aggCtx.accept(parseValueMap(raw));
        }
        Map<Aggregate, Integer> aggregateIndex = new HashMap<>();
        if (select.aggregates != null) {
            for (int i = 0; i < select.aggregates.length; i++) {
                aggregateIndex.put(select.aggregates[i], i);
            }
        }
        List<String> aggValues = aggCtx.stringValues();
        List<String> aggLabels = aggCtx.labels();
        List<String> headers = new ArrayList<>();
        List<String> row = new ArrayList<>();
        List<Select.Item> projection = select.projection != null
                ? Arrays.asList(select.projection)
                : Collections.emptyList();

        for (Select.Item item : projection) {
            int idx = aggregateIndex.get(item.aggregate);
            headers.add(item.alias != null ? item.alias : aggLabels.get(idx));
            row.add(aggValues.get(idx));
        }
        List<List<String>> rows = new ArrayList<>();
        rows.add(row);
        return new ResultSet(headers, rows);
    }

    /**
     * GROUP BY 的分组键
     */
    private ResultSet aggregateByGroup(long xid, Select select, List<Long> uids) throws Exception {
        // 分组聚合结果
        Map<GroupingKey, AggregateContext> grouped = new HashMap<>();
        List<Field> groupingFields = getGroupingFields(select.groupBy);
        // 分组键
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            // 解码为 {字段名 → 值} 的映射
            Map<String, Object> row = parseValueMap(raw);
            // 根据 GROUP BY 字段值构造分组键
            GroupingKey key = new GroupingKey(row, groupingFields, select.groupBy);
            // 找到当前 group 对应的聚合器
            AggregateContext aggCtx = grouped.get(key);
            if (aggCtx == null) {
                // 首次出现该 group，构造聚合器集合
                aggCtx = AggregateContext.of(fields, select.aggregates);
                grouped.put(key, aggCtx);
            }
            // 将当前数据行融合进该组的聚合统计中
            aggCtx.accept(row);
        }

        // 构建聚合结果表头
        List<Select.Item> projection = select.projection != null
                ? Arrays.asList(select.projection)
                : Collections.emptyList();

        Map<Aggregate, Integer> aggregateIndex = new HashMap<>();
        if (select.aggregates != null) {
            for (int i = 0; i < select.aggregates.length; i++) {
                aggregateIndex.put(select.aggregates[i], i);
            }
        }
        AggregateContext sample = grouped.values().stream().findFirst().orElse(null);
        List<String> aggLabels = sample != null
                ? sample.labels()
                : AggregateContext.of(fields, select.aggregates).labels();

        List<String> headers = new ArrayList<>();
        for (Select.Item item : projection) {
            if (item.isAggregate()) {
                int idx = aggregateIndex.get(item.aggregate);
                headers.add(item.alias != null ? item.alias : aggLabels.get(idx));
            } else {
                headers.add(item.alias != null ? item.alias : item.field);
            }
        }

        // 构建聚合结果行
        List<List<String>> rows = new ArrayList<>();
        for (Map.Entry<GroupingKey, AggregateContext> entry : grouped.entrySet()) {
            GroupingKey key = entry.getKey();
            AggregateContext aggCtx = entry.getValue();
            List<Object> aggValues = aggCtx.values();
            if (select.having != null && !matchHaving(select.having, key, aggValues)) {
                continue;
            }
            List<String> row = new ArrayList<>();
            List<String> aggStringValues = aggCtx.stringValues();
            for (Select.Item item : projection) {
                if (item.isAggregate()) {
                    int idx = aggregateIndex.get(item.aggregate);
                    row.add(aggStringValues.get(idx));
                } else {
                    Object v = key.getValue(item.field);
                    row.add(v == null ? "NULL" : v.toString());
                }
            }
            rows.add(row);
        }
        return new ResultSet(headers, rows);
    }

    /**
     * 无聚合，仅 GROUP BY 去重
     */
    private ResultSet distinctByGroup(long xid, Select select, List<Long> uids) throws Exception {
        List<Field> groupingFields = getGroupingFields(select.groupBy);
        // 按出现顺序保留分组
        Set<GroupingKey> grouped = new LinkedHashSet<>();
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> valueMap = parseValueMap(raw);
            GroupingKey key = new GroupingKey(valueMap, groupingFields, select.groupBy);
            grouped.add(key);
        }

        List<Select.Item> projection = select.projection != null
                ? Arrays.asList(select.projection)
                : Collections.emptyList();
        List<String> headers = new ArrayList<>();
        for (Select.Item item : projection) {
            headers.add(item.alias != null ? item.alias : item.field);
        }

        List<List<String>> rows = new ArrayList<>();
        grouped.forEach(key -> {
            List<String> row = new ArrayList<>();
            for (Select.Item item : projection) {
                Object v = key.getValue(item.field);
                row.add(v == null ? "NULL" : v.toString());
            }
            rows.add(row);
        });
        return new ResultSet(headers, rows);
    }

    /**
     * 分组键
     * 重写 equals() 和 hashCode()
     */
    private static class GroupingKey {
        private final String[] names;
        private final FieldType[] types;
        private final Object[] values;
        private final Map<String, Integer> index;

        GroupingKey(Map<String, Object> valueMap, List<Field> fields, String[] groupBy) throws Exception {
            int len = groupBy.length;
            names = new String[len];
            types = new FieldType[len];
            values = new Object[len];
            index = new HashMap<>();
            for (int i = 0; i < len; i++) {
                String name = groupBy[i];
                names[i] = name;
                types[i] = fields.get(i).fieldType;
                values[i] = valueMap.get(name);
                index.put(name, i);
            }
        }

        Object getValue(int idx) {
            return values[idx];
        }

        FieldType getType(int idx) {
            return types[idx];
        }

        Object getValue(String name) {
            Integer idx = index.get(name);
            if (idx == null)
                return null;
            return values[idx];
        }

        FieldType getType(String name) {
            Integer idx = index.get(name);
            if (idx == null)
                return null;
            return types[idx];
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof GroupingKey))
                return false;
            GroupingKey other = (GroupingKey) o;
            return Arrays.equals(values, other.values);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(values);
        }
    }

    /**
     * 匹配 HAVING 后的 Condition 树
     */
    private boolean matchHaving(Condition condition,
            GroupingKey key, List<Object> aggregateValues) throws Exception {
        if (condition == null)
            return true;
        return matchCondition(condition, key, aggregateValues);
    }

    private boolean matchCondition(Condition condition,
            GroupingKey key, List<Object> aggregateValues) throws Exception {
        if (condition instanceof PredicateCondition) {
            return matchPredicateCondition((PredicateCondition) condition, key, aggregateValues);
        }
        if (condition instanceof BinaryCondition) {
            BinaryCondition binary = (BinaryCondition) condition;
            if (binary.lop == LogicOperator.AND) {
                return matchCondition(binary.left, key, aggregateValues)
                        && matchCondition(binary.right, key, aggregateValues);
            }
            if (binary.lop == LogicOperator.OR) {
                return matchCondition(binary.left, key, aggregateValues)
                        || matchCondition(binary.right, key, aggregateValues);
            }
        }
        return false;
    }

    private boolean matchPredicateCondition(PredicateCondition predicate,
            GroupingKey key, List<Object> aggregateValues) {
        Object left = resolveOperand(predicate.left, key, aggregateValues);
        Object right = resolveOperand(predicate.right, key, aggregateValues);
        if (left == null || right == null) {
            return false;
        }
        int cmp = compareValues(left, right);
        return predicate.cop.match(cmp);
    }

    private Object resolveOperand(Operand operand, GroupingKey key, List<Object> aggregateValues) {
        switch (operand.kind) {
            case COLUMN:
                int idx = operand.groupIndex;
                if (idx < 0 || idx >= key.values.length) {
                    return null;
                }
                return key.values[idx];
            case AGGREGATE:
                if (operand.aggregateIndex < 0 || operand.aggregateIndex >= aggregateValues.size()) {
                    return null;
                }
                return aggregateValues.get(operand.aggregateIndex);
            case CONSTANT:
            default:
                return operand.constant;
        }
    }

    private int compareValues(Object left, Object right) {
        if (left instanceof Number && right instanceof Number) {
            double l = ((Number) left).doubleValue();
            double r = ((Number) right).doubleValue();
            return Double.compare(l, r);
        }
        return left.toString().compareTo(right.toString());
    }

    private List<Field> getGroupingFields(String[] groupBy) throws Exception {
        List<Field> groupingKeyFields = new ArrayList<>();
        for (String col : groupBy) {
            Field field = getFieldByName(col);
            if (field == null) {
                throw Error.FieldNotFoundException;
            }
            groupingKeyFields.add(field);
        }
        return groupingKeyFields;
    }

    // =========================================================
    // WHERE / 索引优化 / UID 选取
    // =========================================================

    /**
     * 解析 WHERE 条件，结合索引情况做优化：
     *
     * 索引情况 AND OR
     * a✔ b✔ 两索引各自查，取交集 两索引各自查，取并集
     * a✔ b✘ 用 a 索引做候选，再过滤 b 用 a 索引查 a，再全表扫描匹配 b，最后并集
     * a✘ b✔ 用 b 索引做候选，再过滤 a 用 b 索引查 b，再全表扫描匹配 a，最后并集
     * a✘ b✘ 全表扫描 + matchWhere 全表扫描 + matchWhere
     *
     * 同时保留：
     * - 单表达式优化（一个字段）
     * - 同字段多表达式的 calWhere 优化
     * - 出现 "!=" 统一退回全表扫描保证正确性
     */
    private List<Long> applyWhere(long xid, Where where) throws Exception {
        // 1. 没有 WHERE：全表扫描
        if (where == null) {
            return getUidsByWhere(xid, null);
        }

        // 2. 出现 "!="：退回全表扫描 + matchWhere
        boolean hasNotEqual = (where.singleExp1 != null && where.singleExp1.op == CompareOperator.NE)
                || (where.singleExp2 != null && where.singleExp2.op == CompareOperator.NE);
        if (hasNotEqual) {
            return getUidsByWhere(xid, where);
        }

        // 3. 只有一个表达式：单字段优化
        LogicOperator logicOperator = LogicOperator.from(where.logicOp);

        if (where.singleExp2 == null || logicOperator == LogicOperator.NONE) {
            SingleExpression exp = where.singleExp1;
            Field f = getFieldByName(exp.field);
            if (f == null)
                throw Error.FieldNotFoundException;

            if (f.isIndexed()) {
                Range r = f.computeExpression(exp);
                List<Long> uids = f.search(r.getLeft(), r.getRight());
                Set<Long> uidSet = new LinkedHashSet<>();
                if (uids != null)
                    uidSet.addAll(uids);
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

        // 4.1 同字段且有索引，继续用 computeWhere 做单字段范围优化
        if (exp1.field.equals(exp2.field) && f1.isIndexed()) {
            List<Range> ranges = computeWhere(f1, where);
            if (ranges.isEmpty()) {
                return new ArrayList<>();
            }
            Set<Long> uidSet = new LinkedHashSet<>();
            for (Range r : ranges) {
                List<Long> uids = f1.search(r.getLeft(), r.getRight());
                if (uids != null)
                    uidSet.addAll(uids);
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
            // 只要有一侧无索引，直接全表扫描 OR 条件，避免重复读
            if (!aIndexed || !bIndexed) {
                return getUidsByWhere(xid, where);
            }
            // 两侧都有索引：各自走索引结果取并集
            Set<Long> result = new LinkedHashSet<>();
            result.addAll(getUidsByIndexAndExp(xid, f1, exp1));
            result.addAll(getUidsByIndexAndExp(xid, f2, exp2));
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

        if (where == null)
            return all;

        List<Long> matched = new ArrayList<>();
        for (Long uid : all) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> row = parseValueMap(raw);
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
    private List<Range> computeWhere(Field fd, Where where) throws Exception {
        List<Range> ranges = new ArrayList<>();
        LogicOperator op = LogicOperator.from(where.logicOp);
        switch (op) {
            case NONE: {
                Range r = fd.computeExpression(where.singleExp1);
                ranges.add(r);
                break;
            }
            case OR: {
                Range r1 = fd.computeExpression(where.singleExp1);
                Range r2 = fd.computeExpression(where.singleExp2);
                ranges.add(r1);
                ranges.add(r2);
                break;
            }
            case AND: {
                Range r1 = fd.computeExpression(where.singleExp1);
                Range r2 = fd.computeExpression(where.singleExp2);
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
            if (field.isUnique() && field.isIndexed()) {
                return field;
            }
        }
        for (Field field : fields) {
            if (field.isIndexed()) {
                return field;
            }
        }
        throw Error.FieldNotIndexedException;
    }

    private boolean matchWhere(Map<String, Object> row, Where where) throws Exception {
        if (where == null) {
            return true;
        }
        LogicOperator op = LogicOperator.from(where.logicOp);
        switch (op) {
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
        Field target = getFieldByName(exp.field);
        if (target == null) {
            throw Error.FieldNotFoundException;
        }
        // 2. 从 row 中拿到该字段的当前实际值
        Object curVal = row.get(target.fieldName);
        if (curVal == null)
            return false;

        FieldType type = target.fieldType;
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
        Range r = f.computeExpression(exp);
        List<Long> uids = f.search(r.getLeft(), r.getRight());
        if (uids == null || uids.isEmpty())
            return res;

        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> row = parseValueMap(raw);
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
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> row = parseValueMap(raw);
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
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            Map<String, Object> row = parseValueMap(raw);
            if (matchWhere(row, where)) {
                res.add(uid);
            }
        }
        return res;
    }

    // =========================================================
    // SELECT 字段解析 / 结果集格式化
    // =========================================================

    /**
     * To ResultSet
     * 将查询结果转为结构化数据（无聚合、无分组）。
     */
    private ResultSet toResultSet(long xid, Select select, List<Long> uids) throws Exception {
        List<Map<String, Object>> valueMapList = new ArrayList<>();
        for (Long uid : uids) {
            byte[] raw = vm.read(xid, uid);
            if (raw == null)
                continue;
            valueMapList.add(parseValueMap(raw));
        }

        List<Select.Item> projection = select.projection != null
                ? Arrays.asList(select.projection)
                : Collections.emptyList();
        List<Field> outputFields = new ArrayList<>();
        List<String> headers = new ArrayList<>();

        if (projection.isEmpty()) {
            for (Field field : fields) {
                outputFields.add(field);
                headers.add(field.fieldName);
            }
        } else {
            for (Select.Item item : projection) {
                String column = item.field;
                if ("*".equals(column)) {
                    for (Field field : fields) {
                        outputFields.add(field);
                        headers.add(field.fieldName);
                    }
                    continue;
                }
                Field matched = getFieldByName(column);
                if (matched == null) {
                    throw Error.FieldNotFoundException;
                }
                outputFields.add(matched);
                headers.add(item.alias != null ? item.alias : matched.fieldName);
            }
        }

        List<List<String>> rows = new ArrayList<>();
        for (Map<String, Object> row : valueMapList) {
            List<String> rowValues = new ArrayList<>();
            for (Field field : outputFields) {
                String value = field.stringValue(row.get(field.fieldName));
                if (value == null)
                    value = "NULL";
                rowValues.add(value);
            }
            rows.add(rowValues);
        }
        return new ResultSet(headers, rows);
    }

    // =========================================================
    // 行编码 / 解码
    // =========================================================

    /**
     * 将一行二进制数据解析为（字段名 → 值）的映射。
     *
     * @param raw 行原始字节
     * @return 字段值映射
     */
    private Map<String, Object> parseValueMap(byte[] raw) {
        int pos = 0;
        Map<String, Object> valueMap = new HashMap<>();
        for (Field field : fields) {
            ParsedValue r = field.parseValue(Arrays.copyOfRange(raw, pos, raw.length));
            valueMap.put(field.fieldName, r.value);
            pos += r.size;
        }
        return valueMap;
    }

    /**
     * 将（字段名 → 值）的映射编码为行二进制。
     *
     * @param valueMap 字段值映射
     * @return 行原始字节
     */
    private byte[] serializeValueMap(Map<String, Object> valueMap) {
        byte[] raw = new byte[0];
        for (Field field : fields) {
            raw = Bytes.concat(raw, field.toRaw(valueMap.get(field.fieldName)));
        }
        return raw;
    }

    // =========================================================
    // 调试输出
    // =========================================================

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append(name).append(": ");
        for (Field field : fields) {
            sb.append(field.toString());
            if (field == fields.get(fields.size() - 1)) {
                sb.append("}");
            } else {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
