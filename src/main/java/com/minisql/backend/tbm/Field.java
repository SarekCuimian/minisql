package com.minisql.backend.tbm;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import com.google.common.primitives.Bytes;

import com.minisql.backend.im.BPlusTree;
import com.minisql.backend.parser.statement.SingleExpression;
import com.minisql.backend.parser.statement.operator.CompareOperator;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.backend.utils.ParsedValue;
import com.minisql.common.Error;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid][UniqueFlag][PrimaryFlag]
 * 如果field无索引，IndexUid为0；
 * UniqueFlag 为 1 表示唯一；
 * PrimaryFlag 为 1 表示为主键；
 */
public class Field {
    long uid;
    private final Table tb;
    public String fieldName;
    public FieldType fieldType;
    // 索引的 uid
    private long index;
    // 唯一标志
    private boolean unique;
    // 主键标志
    private boolean primary;
    private BPlusTree tree;

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = tb.vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.of(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, FieldType fieldType, long index, boolean unique, boolean primary) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
        this.unique = unique;
        this.primary = primary;
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType,
                                    boolean indexed, boolean unique, boolean primary) throws Exception {
        FieldType type = FieldType.from(fieldType);
        if(primary) {
            unique = true;
        }
        if(unique && !indexed) {
            indexed = true;
        }
        Field f = new Field(tb, fieldName, type, 0, unique, primary);
        if(indexed) {
            long index = BPlusTree.create(tb.dm);
            BPlusTree tree = BPlusTree.load(index, tb.dm);
            f.index = index;
            f.tree = tree;
        }
        f.persistSelf(xid);
        return f;
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParsedValue parsed = ByteUtil.parseString(raw);
        fieldName = (String) parsed.value;
        position += parsed.size;
        parsed = ByteUtil.parseString(Arrays.copyOfRange(raw, position, raw.length));
        try {
            fieldType = FieldType.from((String) parsed.value);
        } catch (Exception e) {
            Panic.of(e);
        }
        position += parsed.size;
        this.index = ByteUtil.parseLong(Arrays.copyOfRange(raw, position, position + 8));
        if(index != 0) {
            try {
                tree = BPlusTree.load(index, tb.dm);
            } catch(Exception e) {
                Panic.of(e);
            }
        }
        position += 8;
        if(position < raw.length) {
            unique = raw[position] == (byte)1;
            position += 1;
        } else {
            unique = false;
        }
        if(position < raw.length) {
            primary = raw[position] == (byte)1;
        } else {
            primary = false;
        }
        return this;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = ByteUtil.stringToByte(fieldName);
        byte[] typeRaw = ByteUtil.stringToByte(fieldType.name().toLowerCase(Locale.ROOT));
        byte[] indexRaw = ByteUtil.longToByte(index);
        byte[] uniqueRaw = new byte[] {(byte)(unique?1:0)};
        byte[] primaryRaw = new byte[] {(byte)(primary?1:0)};
        this.uid = tb.vm
                .insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw, uniqueRaw, primaryRaw));
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public boolean isUnique() {
        return unique;
    }

    public boolean isPrimary() {
        return primary;
    }

    public FieldType getType() {
        return fieldType;
    }

    public String getTypeName() {
        return fieldType.name().toLowerCase(Locale.ROOT);
    }

    public String getName() {
        return fieldName;
    }

    public void ensureUnique(long xid, Object value, Long selfUid) throws Exception {
        if(!unique) return;
        long key = toKey(value);
        List<Long> uids = tree.searchRange(key, key);
        if(uids == null || uids.isEmpty()) {
            return;
        }
        TableManagerImpl tm = (TableManagerImpl)tb.tbm;
        for (Long uid : uids) {
            if(selfUid != null && selfUid.equals(uid)) {
                continue;
            }
            byte[] raw = tm.vm.read(xid, uid);
            if(raw != null) {
                throw Error.DuplicatedEntryException;
            }
        }
    }

    public void insert(Object value, long uid) throws Exception {
        long key = toKey(value);
        tree.insert(key, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return tree.searchRange(left, right);
    }

    public Object stringToValue(String str) {
        switch(fieldType) {
            case INT32:
                return Integer.parseInt(str);
            case INT64:
                return Long.parseLong(str);
            case STRING:
                return str;
        }
        return null;
    }

    /**
     * 将字段值转换成B+树的索引键
     */
    public long toKey(Object value) {
        long key = 0;
        switch(fieldType) {
            case STRING:
                // 将字符串转换成自定义哈希值作为BPlusTree的索引key
                key = hashStringKey((String) value);
                break;
            case INT32:
            case INT64:
                key = ((Number) value).longValue();
                break;
        }
        return key;
    }

    private long hashStringKey(String value) {
        long seed = 13331;
        long res = 0;
        for (byte b : value.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }

    public byte[] toRaw(Object value) {
        byte[] raw = null;
        switch(fieldType) {
            case INT32:
                raw = ByteUtil.intToByte((int)value);
                break;
            case INT64:
                raw = ByteUtil.longToByte((long)value);
                break;
            case STRING:
                raw = ByteUtil.stringToByte((String)value);
                break;
        }
        return raw;
    }

    // raw [len][data] 
    public ParsedValue parseValue(byte[] raw) {
        Object value = null;
        int size = 0;
        switch(fieldType) {
            case INT32:
                value = ByteUtil.parseInt(Arrays.copyOf(raw, 4));
                size = 4;
                break;
            case INT64:
                value = ByteUtil.parseLong(Arrays.copyOf(raw, 8));
                size = 8;
                break;
            case STRING:
                return ByteUtil.parseString(raw);
        }
        return new ParsedValue(value, size);
    }

    public String stringValue(Object v) {
        String str = null;
        switch(fieldType) {
            case INT32:
                str = String.valueOf((int)v);
                break;
            case INT64:
                str = String.valueOf((long)v);
                break;
            case STRING:
                str = (String)v;
                break;
        }
        return str;
    }

    public Range computeExpression(SingleExpression exp) throws Exception {
        Object v = stringToValue(exp.value);
        long uid = toKey(v);
        Range range = null;
        CompareOperator op = exp.op;
        switch(op) {
            case LT:
                range = new Range(Long.MIN_VALUE, uid > Long.MIN_VALUE ? uid - 1 : Long.MIN_VALUE);
                break;
            case LE:
                range = new Range(Long.MIN_VALUE, uid);
                break;
            case EQ:
                range = new Range(uid, uid);
                break;
            case GT:
                range = new Range((uid == Long.MAX_VALUE) ? Long.MAX_VALUE : uid + 1, Long.MAX_VALUE);
                break;
            case GE:
                range = new Range(uid, Long.MAX_VALUE);
                break;
            case NE:
                range = new Range(Long.MIN_VALUE, Long.MAX_VALUE);
                break;
            default:
                throw Error.InvalidLogOpException;
        }
        return range;
    }

    @Override
    public String toString() {
        return new StringBuilder("(")
                .append(fieldName)
                .append(", ")
                .append(fieldType.name().toLowerCase(Locale.ROOT))
                .append(index!=0?", Index":", NoIndex")
                .append(unique?", Unique":", NonUnique")
                .append(")")
                .toString();
    }
}
