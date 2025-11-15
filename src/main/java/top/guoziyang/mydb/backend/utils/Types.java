package top.guoziyang.mydb.backend.utils;

public class Types {
    /**
     * 将页号和偏移量组合成 UID [pgno(高32)][offset(低16)]
     *
     * @param pgno 页号
     * @param offset 偏移量
     * @return 组合后的 UID
     */
    public static long addressToUid(int pgno, short offset) {
        
        return ((long)pgno << 32) | ((long)offset & 0xFFFF);
    }
}
