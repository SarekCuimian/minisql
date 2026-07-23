package com.minisql.engine.storage.page;

import java.util.Arrays;

import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.storage.page.RandomUtil;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 */
public final class MetaPage {

    private MetaPage() {
    }

    /**
     * 校验区起始偏移量（第 100 字节）
     */
    private static final int VALIDATION_OFFSET = 100;

    /**
     * 校验区长度（8 字节）
     */
    private static final int VALID_CHECK_SIZE = 8;

    /**
     * 初始化数据库第一页的数据。
     * <p>创建一个空页并在 100~107 字节处写入随机字节，表示数据库被“打开”。</p>
     *
     * @return 初始化后的 page bytes
     */
    public static byte[] newPageBytes() {
        byte[] pageBytes = new byte[PageCache.PAGE_SIZE];
        setVcOpen(pageBytes);
        return pageBytes;
    }

    /**
     * 标记数据库被打开，在指定页的 100~107 字节写入随机字节。
     *
     * @param pg 页对象
     */
    public static void setVcOpen(Page pg) {
        pg.wLock();
        try {
            pg.setDirty(true);
            setVcOpen(pg.getBytes());
        } finally {
            pg.wUnlock();
        }
    }

    /**
     * 在给定字节数组的 100~107 字节写入随机校验值。
     *
     * @param pageBytes 页字节数组
     */
    private static void setVcOpen(byte[] pageBytes) {
        System.arraycopy(
                RandomUtil.randomBytes(VALID_CHECK_SIZE), 0,
                pageBytes, VALIDATION_OFFSET,
                VALID_CHECK_SIZE
        );
    }

    /**
     * 标记数据库被正常关闭。
     * <p>将偏移 100~107 的随机字节复制到 108~115。</p>
     *
     * @param pg 页对象
     */
    public static void setVcClose(Page pg) {
        pg.wLock();
        try {
            pg.setDirty(true);
            setVcClose(pg.getBytes());
        } finally {
            pg.wUnlock();
        }
    }

    /**
     * 将偏移 100~107 的内容复制到 108~115。
     * <p>用于记录“数据库安全关闭”的状态。</p>
     *
     * @param pageBytes 页字节数组
     */
    private static void setVcClose(byte[] pageBytes) {
        System.arraycopy(
                pageBytes, VALIDATION_OFFSET,
                pageBytes, VALIDATION_OFFSET + VALID_CHECK_SIZE,
                VALID_CHECK_SIZE
        );
    }

    /**
     * 检查数据库上一次是否正常关闭。
     *
     * @param pg 页对象
     * @return 若 100~107 与 108~115 字节内容一致，返回 {@code true}；
     * 否则返回 {@code false}
     */
    public static boolean checkVc(Page pg) {
        pg.rLock();
        try {
            return checkVc(pg.getBytes());
        } finally {
            pg.rUnlock();
        }
    }

    /**
     * 对字节数组进行校验比较。
     *
     * @param pageBytes 页字节数组
     * @return {@code true} 表示数据库上次安全关闭；{@code false} 表示异常退出
     */
    private static boolean checkVc(byte[] pageBytes) {
        return Arrays.equals(
                Arrays.copyOfRange(pageBytes, VALIDATION_OFFSET, VALIDATION_OFFSET + VALID_CHECK_SIZE),
                Arrays.copyOfRange(pageBytes, VALIDATION_OFFSET + VALID_CHECK_SIZE, VALIDATION_OFFSET + 2 * VALID_CHECK_SIZE)
        );
    }
}
