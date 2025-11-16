package com.minisql.backend.dm.page;

import java.util.Arrays;

import com.minisql.backend.dm.pageCache.PageCache;
import com.minisql.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {

    /** FSO 起始偏移（字节）。固定为 0。 */
    private static final short OF_FREE = 0;

    /** 数据区起始偏移（字节）。固定为 2。 */
    private static final short OF_DATA = 2;

    /**
     * 单页可用最大空闲空间（字节）。
     * <p>等于页大小减去 FSO 占用的 2 字节。</p>
     */
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    /**
     * 初始化一个普通页的原始字节数组，并设置 FSO 指向数据区起始处（偏移 2）。
     *
     * @return 已初始化的页原始数据（长度为 {@link PageCache#PAGE_SIZE}）
     */
    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 将给定页原始数据的 FSO 设置为指定偏移。
     * <p><b>注意：</b>本方法不做越界与合法性校验。</p>
     *
     * @param raw    页原始字节数组
     * @param ofData 期望的空闲空间起始偏移（通常 ≥ {@link #OF_DATA}）
     */
    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 读取页面的 FSO（空闲空间起始偏移）。
     *
     * @param pg 页对象
     * @return FSO 偏移值
     */
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    /**
     * 从原始字节数组中读取 FSO（空闲空间起始偏移）。
     *
     * @param raw 页原始字节数组
     * @return FSO 偏移值
     */
    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将 {@code raw} 数据顺序追加写入到页的空闲区域（从当前 FSO 开始），并推进 FSO。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>读取当前 FSO；</li>
     *   <li>将 {@code raw} 拷贝到 {@code [FSO, FSO + raw.length)}；</li>
     *   <li>将 FSO 更新为 {@code FSO + raw.length}；</li>
     * </ol>
     *
     * <p><b>注意：</b>调用前须确保 {@code raw.length} 不会超出页剩余空间；本方法不做越界校验。</p>
     *
     * @param pg  目标页
     * @param raw 待写入的数据
     * @return 写入的起始偏移（即旧的 FSO）
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        setFSO(pg.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 计算页面当前空闲空间大小（字节）。
     * <p>等于 {@code PageCache.PAGE_SIZE - FSO}。</p>
     *
     * @param pg 目标页
     * @return 空闲空间字节数
     */
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int) getFSO(pg.getData());
    }

    /**
     * 恢复流程下的“插入重放”：在给定 {@code offset} 位置写入 {@code raw}，
     * 并且若 {@code offset + raw.length} 超过当前 FSO，则推进 FSO。
     *
     * <p>用于 WAL/Redo 等恢复场景，保证页面元信息与数据区一致。</p>
     *
     * @param pg     目标页
     * @param raw    待写入的数据
     * @param offset 指定的写入起始偏移
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        short rawFSO = getFSO(pg.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 恢复流程下的“更新重放”：在给定 {@code offset} 位置写入 {@code raw}，
     * <b>但不</b>调整 FSO（适用于覆盖式更新场景）。
     *
     * @param pg     目标页
     * @param raw    待写入的数据
     * @param offset 指定的写入起始偏移
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}