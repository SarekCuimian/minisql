package com.minisql.backend.tbm;

import java.util.Arrays;
import java.util.List;

import com.google.common.primitives.Bytes;

import com.minisql.backend.im.BPlusTree;
import com.minisql.backend.parser.statement.SingleExpression;
import com.minisql.backend.txm.TransactionManagerImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.ParseStringRes;
import com.minisql.backend.utils.ByteUtil;
import com.minisql.common.Error;

/**
 * field 表示字段信息
 * 二进制格式为：
 * [FieldName][TypeName][IndexUid][UniqueFlag]
 * 如果field无索引，IndexUid为0；UniqueFlag 为 1 表示唯一
 */
public class Field {
    long uid;
    private Table tb;
    String fieldName;
    String fieldType;
    private long index;
    private boolean unique;
    private BPlusTree bt;

    public static Field loadField(Table tb, long uid) {
        byte[] raw = null;
        try {
            raw = ((TableManagerImpl)tb.tbm).vm.read(TransactionManagerImpl.SUPER_XID, uid);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert raw != null;
        return new Field(uid, tb).parseSelf(raw);
    }

    public Field(long uid, Table tb) {
        this.uid = uid;
        this.tb = tb;
    }

    public Field(Table tb, String fieldName, String fieldType, long index, boolean unique) {
        this.tb = tb;
        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.index = index;
        this.unique = unique;
    }

    private Field parseSelf(byte[] raw) {
        int position = 0;
        ParseStringRes res = ByteUtil.parseString(raw);
        fieldName = res.str;
        position += res.next;
        res = ByteUtil.parseString(Arrays.copyOfRange(raw, position, raw.length));
        fieldType = res.str;
        position += res.next;
        this.index = ByteUtil.parseLong(Arrays.copyOfRange(raw, position, position+8));
        if(index != 0) {
            try {
                bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            } catch(Exception e) {
                Panic.panic(e);
            }
        }
        position += 8;
        if(position < raw.length) {
            unique = raw[position] == (byte)1;
        } else {
            unique = false;
        }
        return this;
    }

    public static Field createField(Table tb, long xid, String fieldName, String fieldType, boolean indexed, boolean unique) throws Exception {
        typeCheck(fieldType);
        if(unique && !indexed) {
            indexed = true;
        }
        Field f = new Field(tb, fieldName, fieldType, 0, unique);
        if(indexed) {
            long index = BPlusTree.create(((TableManagerImpl)tb.tbm).dm);
            BPlusTree bt = BPlusTree.load(index, ((TableManagerImpl)tb.tbm).dm);
            f.index = index;
            f.bt = bt;
        }
        f.persistSelf(xid);
        return f;
    }

    private void persistSelf(long xid) throws Exception {
        byte[] nameRaw = ByteUtil.string2Byte(fieldName);
        byte[] typeRaw = ByteUtil.string2Byte(fieldType);
        byte[] indexRaw = ByteUtil.long2Byte(index);
        byte[] uniqueRaw = new byte[] {(byte)(unique?1:0)};
        this.uid = ((TableManagerImpl)tb.tbm).vm.insert(xid, Bytes.concat(nameRaw, typeRaw, indexRaw, uniqueRaw));
    }

    private static void typeCheck(String fieldType) throws Exception {
        if(!"int32".equals(fieldType) && !"int64".equals(fieldType) && !"string".equals(fieldType)) {
            throw Error.InvalidFieldException;
        }
    }

    public boolean isIndexed() {
        return index != 0;
    }

    public boolean isUnique() {
        return unique;
    }

    public void ensureUnique(Object key, Long selfUid) throws Exception {
        if(!unique) return;
        long uKey = value2Uid(key);
        List<Long> uids = bt.searchRange(uKey, uKey);
        if(uids == null || uids.isEmpty()) {
            return;
        }
        TableManagerImpl tm = (TableManagerImpl)tb.tbm;
        for (Long uid : uids) {
            if(selfUid != null && selfUid.equals(uid)) {
                continue;
            }
            byte[] raw = tm.vm.read(TransactionManagerImpl.SUPER_XID, uid);
            if(raw != null) {
                throw Error.DuplicatedEntryException;
            }
        }
    }

    public void insert(Object key, long uid) throws Exception {
        long uKey = value2Uid(key);
        bt.insert(uKey, uid);
    }

    public List<Long> search(long left, long right) throws Exception {
        return bt.searchRange(left, right);
    }

    public Object string2Value(String str) {
        switch(fieldType) {
            case "int32":
                return Integer.parseInt(str);
            case "int64":
                return Long.parseLong(str);
            case "string":
                return str;
        }
        return null;
    }

    public long value2Uid(Object key) {
        long uid = 0;
        switch(fieldType) {
            case "string":
                uid = ByteUtil.str2Uid((String)key);
                break;
            case "int32":
                int uint = (int)key;
                return (long)uint;
            case "int64":
                uid = (long)key;
                break;
        }
        return uid;
    }

    public byte[] value2Raw(Object v) {
        byte[] raw = null;
        switch(fieldType) {
            case "int32":
                raw = ByteUtil.int2Byte((int)v);
                break;
            case "int64":
                raw = ByteUtil.long2Byte((long)v);
                break;
            case "string":
                raw = ByteUtil.string2Byte((String)v);
                break;
        }
        return raw;
    }

    class ParseValueRes {
        Object v;
        int shift;
    }

    public ParseValueRes parserValue(byte[] raw) {
        ParseValueRes res = new ParseValueRes();
        switch(fieldType) {
            case "int32":
                res.v = ByteUtil.parseInt(Arrays.copyOf(raw, 4));
                res.shift = 4;
                break;
            case "int64":
                res.v = ByteUtil.parseLong(Arrays.copyOf(raw, 8));
                res.shift = 8;
                break;
            case "string":
                ParseStringRes r = ByteUtil.parseString(raw);
                res.v = r.str;
                res.shift = r.next;
                break;
        }
        return res;
    }

    public String printValue(Object v) {
        String str = null;
        switch(fieldType) {
            case "int32":
                str = String.valueOf((int)v);
                break;
            case "int64":
                str = String.valueOf((long)v);
                break;
            case "string":
                str = (String)v;
                break;
        }
        return str;
    }

    public Range calExp(SingleExpression exp) throws Exception {
        Object v = string2Value(exp.value);
        long uid = value2Uid(v);
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
                .append(fieldType)
                .append(index!=0?", Index":", NoIndex")
                .append(unique?", Unique":", NonUnique")
                .append(")")
                .toString();
    }
}
