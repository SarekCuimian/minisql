package com.minisql.backend.dm.page;

import java.util.Arrays;

import com.minisql.backend.dm.pageCache.PageCache;
import com.minisql.backend.utils.RandomUtil;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public class PageOne {

    /** 校验区起始偏移量（第 100 字节） */
    private static final int OF_VC = 100;

    /** 校验区长度（8 字节） */
    private static final int LEN_VC = 8;

    /**
     * 初始化数据库第一页的数据。
     * <p>创建一个空页并在 100~107 字节处写入随机字节，表示数据库被“打开”。</p>
     *
     * @return 初始化后的原始页数据字节数组
     */
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    /**
     * 标记数据库被打开，在指定页的 100~107 字节写入随机字节。
     *
     * @param pg 页对象
     */
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    /**
     * 在给定字节数组的 100~107 字节写入随机校验值。
     *
     * @param raw 页数据字节数组
     */
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 标记数据库被正常关闭。
     * <p>将偏移 100~107 的随机字节复制到 108~115。</p>
     *
     * @param pg 页对象
     */
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    /**
     * 将偏移 100~107 的内容复制到 108~115。
     * <p>用于记录“数据库安全关闭”的状态。</p>
     *
     * @param raw 页数据字节数组
     */
    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    /**
     * 检查数据库上一次是否正常关闭。
     *
     * @param pg 页对象
     * @return 若 100~107 与 108~115 字节内容一致，返回 {@code true}；
     *         否则返回 {@code false}
     */
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    /**
     * 对字节数组进行校验比较。
     *
     * @param raw 页数据字节数组
     * @return {@code true} 表示数据库上次安全关闭；{@code false} 表示异常退出
     */
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC)
        );
    }
}

