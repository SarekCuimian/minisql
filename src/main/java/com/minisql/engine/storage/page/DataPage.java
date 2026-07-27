package com.minisql.engine.storage.page;

import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.storage.codec.ByteUtil;

/**
 * PageX管理普通页
 * 普通页结构
 * [PageLsn 8B] [FreeSpaceOffset 2B] [Data]
 */
public final class DataPage {

    private DataPage() {
    }

    /** FSO 字段在 Page 中的起始偏移。 */
    private static final short FSO_OFFSET = PageHeader.HEADER_SIZE;

    /** physical record 区在 Page 中的起始偏移。 */
    public static final short RECORD_AREA_OFFSET =
            PageHeader.HEADER_SIZE + Short.BYTES;

    /**
     * 单页可用最大空闲空间（字节）。
     * <p>等于页大小减去 FSO 占用的 2 字节。</p>
     */
    public static final int MAX_FREE_SPACE_SIZE = PageBufferPool.PAGE_SIZE - RECORD_AREA_OFFSET;

    /**
     * 创建普通页，并设置 FSO 指向公共页头和 DataPage header 之后。
     *
     * @return 已初始化的 page bytes（长度为 {@link PageBufferPool#PAGE_SIZE}）
     */
    public static byte[] newPageBytes() {
        byte[] pageBytes = new byte[PageBufferPool.PAGE_SIZE];
        setFso(pageBytes, RECORD_AREA_OFFSET);
        return pageBytes;
    }

    /**
     * 将给定 page bytes 的 FSO 设置为指定偏移。
     * <p><b>注意：</b>本方法不做越界与合法性校验。</p>
     *
     * @param pageBytes 页字节数组
     * @param fso 期望的空闲空间起始偏移（通常 ≥ {@link #RECORD_AREA_OFFSET}）
     */
    private static void setFso(byte[] pageBytes, short fso) {
        ByteUtil.putShort(pageBytes, FSO_OFFSET, fso);
    }

    /**
     * 读取页面的 FSO（空闲空间起始偏移）。
     *
     * @param pg 页对象
     * @return FSO 偏移值
     */
    public static short getFso(Page pg) {
        pg.rLock();
        try {
            return getFso(pg.getBytes());
        } finally {
            pg.rUnlock();
        }
    }

    /**
     * 从 page bytes 中读取 FSO（空闲空间起始偏移）。
     *
     * @param pageBytes 页字节数组
     * @return FSO 偏移值
     */
    private static short getFso(byte[] pageBytes) {
        return ByteUtil.getShort(pageBytes, FSO_OFFSET);
    }

    /**
     * 将完整 Record bytes 顺序追加写入到页的空闲区域（从当前 FSO 开始），并推进 FSO。
     *
     * <p>流程：</p>
     * <ol>
     *   <li>读取当前 FSO；</li>
     *   <li>将 recordBytes 拷贝到 {@code [FSO, FSO + recordBytes.length)}；</li>
     *   <li>将 FSO 更新为 {@code FSO + recordBytes.length}；</li>
     * </ol>
     *
     * <p><b>注意：</b>调用前须确保 recordBytes.length 不会超出页剩余空间；本方法不做越界校验。</p>
     *
     * @param pg  目标页
     * @param recordBytes 待写入的完整 Record bytes
     * @return 写入的起始偏移（即旧的 FSO）
     */
    public static short insert(Page pg, byte[] recordBytes) {
        pg.wLock();
        try {
            pg.setDirty(true);
            short offset = getFso(pg.getBytes());
            System.arraycopy(recordBytes, 0, pg.getBytes(), offset, recordBytes.length);
            setFso(pg.getBytes(), (short) (offset + recordBytes.length));
            return offset;
        } finally {
            pg.wUnlock();
        }
    }

    /**
     * 计算页面当前空闲空间大小（字节）。
     * <p>等于 {@code PageBufferPool.PAGE_SIZE - FSO}。</p>
     *
     * @param pg 目标页
     * @return 空闲空间字节数
     */
    public static int getFreeSpaceSize(Page pg) {
        pg.rLock();
        try {
            return PageBufferPool.PAGE_SIZE - (int) getFso(pg.getBytes());
        } finally {
            pg.rUnlock();
        }
    }

    /** 从持久化 Page bytes 计算剩余空间。 */
    static int getFreeSpaceSize(byte[] pageBytes) {
        return PageBufferPool.PAGE_SIZE - (int) getFso(pageBytes);
    }

    /**
     * 恢复流程下的“插入重放”：在给定 {@code offset} 位置写入 recordBytes，
     * 并且若 {@code offset + recordBytes.length} 超过当前 FSO，则推进 FSO。
     *
     * <p>用于 WAL/Redo 等恢复场景，保证页面元信息与数据区一致。</p>
     *
     * @param pg     目标页
     * @param recordBytes 待写入的完整 Record bytes
     * @param offset 指定的写入起始偏移
     */
    public static void recoverInsert(Page pg, byte[] recordBytes, short offset) {
        pg.wLock();
        try {
            pg.setDirty(true);
            System.arraycopy(recordBytes, 0, pg.getBytes(), offset, recordBytes.length);

            short currentFso = getFso(pg.getBytes());
            if (currentFso < offset + recordBytes.length) {
                setFso(pg.getBytes(), (short) (offset + recordBytes.length));
            }
        } finally {
            pg.wUnlock();
        }
    }

    /**
     * 恢复流程下的“更新重放”：在给定 {@code offset} 位置写入 recordBytes，
     * <b>但不</b>调整 FSO（适用于覆盖式更新场景）。
     *
     * @param pg     目标页
     * @param recordBytes 待写入的完整 Record bytes
     * @param offset 指定的写入起始偏移
     */
    public static void recoverUpdate(Page pg, byte[] recordBytes, short offset) {
        pg.wLock();
        try {
            pg.setDirty(true);
            System.arraycopy(recordBytes, 0, pg.getBytes(), offset, recordBytes.length);
        } finally {
            pg.wUnlock();
        }
    }
}
