package com.minisql.backend.utils;

public class UidUtil {
    /**
     * 将页号和偏移量组合成 UID 
     * [pgno(32)][reserved(16)][offset(16)]
     * 
     * @param pgno 页号
     * @param offset 偏移量
     * @return 组合后的 UID
     */
    public static long addressToUid(int pgno, short offset) {
        // pgno = 5
        // offset = 300 (0x012C)
        // uid = (5 << 32) | 0x012C
        // uid = 0x 0000 0005 0000 012C
        // offset = uid & 0xFFFF = 0x012C = 300
        // pgno = uid >>> 32 = 5
        return ((long)pgno << 32) | ((long)offset & 0xFFFF);
    }
}
