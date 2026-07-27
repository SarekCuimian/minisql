package com.minisql.engine.table;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

import com.minisql.engine.index.BPlusTree;
import com.minisql.engine.sql.ast.expression.SingleExpression;
import com.minisql.engine.sql.ast.operator.CompareOperator;
import com.minisql.engine.storage.codec.ByteReader;
import com.minisql.engine.storage.codec.ByteWriter;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.error.Panic;
import com.minisql.error.Error;

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

    public static Field load(Table tb, long uid) {
        byte[] fieldBytes = null;
        try {
            fieldBytes = tb.vm.read(XidAllocator.SYSTEM_XID, uid);
        } catch (Exception e) {
            Panic.of(e);
        }
        assert fieldBytes != null;
        return new Field(uid, tb).parse(fieldBytes);
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
            long index = BPlusTree.create(tb.pageRecordManager);
            BPlusTree tree = BPlusTree.load(index, tb.pageRecordManager);
            f.index = index;
            f.tree = tree;
        }
        f.persist(xid);
        return f;
    }

    /** 解析持久化的 Field metadata，并回填当前对象。 */
    private Field parse(byte[] fieldBytes) {
        ByteReader reader = ByteReader.wrap(fieldBytes);
        fieldName = readLengthPrefixedUtf8(reader);
        String fieldTypeName = readLengthPrefixedUtf8(reader);
        try {
            fieldType = FieldType.from(fieldTypeName);
        } catch (Exception e) {
            Panic.of(e);
        }
        this.index = reader.readLong();
        if(index != 0) {
            try {
                tree = BPlusTree.load(index, tb.pageRecordManager);
            } catch(Exception e) {
                Panic.of(e);
            }
        }
        // 兼容旧格式：早期 Field metadata 不包含这两个尾部标志。
        unique = reader.hasRemaining() && reader.readBoolean();
        primary = reader.hasRemaining() && reader.readBoolean();
        reader.requireFullyConsumed();
        return this;
    }

    /** 编码当前 Field metadata 并持久化到 VersionManager。 */
    private void persist(long xid) throws Exception {
        byte[] nameBytes = fieldName.getBytes(StandardCharsets.UTF_8);
        byte[] typeBytes = fieldType.name()
                .toLowerCase(Locale.ROOT)
                .getBytes(StandardCharsets.UTF_8);
        int encodedSize = Integer.BYTES + nameBytes.length
                + Integer.BYTES + typeBytes.length
                + Long.BYTES
                + 2 * Byte.BYTES;
        ByteWriter writer = ByteWriter.allocate(encodedSize);
        writeLengthPrefixedBytes(writer, nameBytes);
        writeLengthPrefixedBytes(writer, typeBytes);
        writer.writeLong(index);
        writer.writeBoolean(unique);
        writer.writeBoolean(primary);
        this.uid = tb.vm.insert(xid, writer.toByteArray());
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
        TableManager tm = tb.tbm;
        for (Long uid : uids) {
            if(selfUid != null && selfUid.equals(uid)) {
                continue;
            }
            byte[] recordBytes = tm.vm.read(xid, uid);
            if(recordBytes != null) {
                throw Error.DuplicatedEntryException;
            }
        }
    }

    public void insert(Object value, long uid) throws Exception {
        long key = toKey(value);
        tree.insert(key, uid);
    }

    public List<Long> searchRange(long left, long right) throws Exception {
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

    /** 将字段值编码为存入行记录的 value bytes。 */
    public byte[] encodeValue(Object value) {
        switch(fieldType) {
            case INT32:
                ByteWriter intWriter = ByteWriter.allocate(Integer.BYTES);
                intWriter.writeInt((int) value);
                return intWriter.toByteArray();
            case INT64:
                ByteWriter longWriter = ByteWriter.allocate(Long.BYTES);
                longWriter.writeLong((long) value);
                return longWriter.toByteArray();
            case STRING:
                byte[] stringBytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                ByteWriter stringWriter = ByteWriter.allocate(Integer.BYTES + stringBytes.length);
                writeLengthPrefixedBytes(stringWriter, stringBytes);
                return stringWriter.toByteArray();
            default:
                throw new IllegalStateException("Unsupported field type: " + fieldType);
        }
    }

    /** 从行数据 reader 解析当前字段值；字符串布局为 {@code [Length][Data]}。 */
    public Object parseValue(ByteReader reader) {
        switch(fieldType) {
            case INT32:
                return reader.readInt();
            case INT64:
                return reader.readLong();
            case STRING:
                return readLengthPrefixedUtf8(reader);
            default:
                throw new IllegalStateException("Unsupported field type: " + fieldType);
        }
    }

    private static String readLengthPrefixedUtf8(ByteReader reader) {
        int byteLength = reader.readInt();
        return reader.readUtf8(byteLength);
    }

    private static void writeLengthPrefixedBytes(ByteWriter writer, byte[] value) {
        writer.writeInt(value.length);
        writer.writeBytes(value);
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
