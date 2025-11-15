package top.guoziyang.mydb.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    /** ValidFlag 字节偏移量 */
    static final int OF_VALID = 0;
    /** DataSize 起始偏移量 */
    static final int OF_SIZE = 1;
    /** Data 区域起始偏移量 */
    static final int OF_DATA = 3;

    /** 当前 DataItem 在页面中的数据视图（包含头部与数据） */
    private SubArray raw;
    /** 修改前的旧数据，用于回滚或 WAL before image */
    private byte[] oldRaw;
    /** 读锁 */
    private Lock rLock;
    /** 写锁 */
    private Lock wLock;
    /** 所属 DataManager 实例 */
    private DataManagerImpl dm;
    /** 全局唯一标识 UID（pageNo + offset） */
    private long uid;
    /** 当前 DataItem 所在的 Page */
    private Page pg;

    /**
     * 构造函数
     *
     * @param raw   当前 DataItem 在页面中的字节切片
     * @param oldRaw 修改前的备份字节数组
     * @param pg    所在的 Page
     * @param uid   唯一标识
     * @param dm    所属 DataManager
     */
    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    /**
     * 判断当前 DataItem 是否有效。
     *
     * @return true 表示有效，false 表示无效
     */
    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    /**
     * 获取数据区（不包含头部信息）的 SubArray 视图。
     *
     * @return 数据区视图
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    /**
     * 修改前调用：
     * <ul>
     *   <li>加写锁</li>
     *   <li>设置页面为 dirty</li>
     *   <li>复制当前数据到 oldRaw（备份旧值）</li>
     * </ul>
     */
    @Override
    public void before() {
        wLock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    /**
     * 在修改失败或异常回滚时调用：
     * <ul>
     *   <li>将 oldRaw 拷贝回原数据位置，恢复修改前的状态</li>
     *   <li>释放写锁</li>
     * </ul>
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    /**
     * 在修改成功时调用：
     * <ul>
     *   <li>调用 DataManager 记录 WAL 日志</li>
     *   <li>释放写锁</li>
     * </ul>
     *
     * @param xid 当前事务 ID
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this);
        wLock.unlock();
    }

    /**
     * 释放该 DataItem 的引用，
     * 通常在缓存淘汰或事务结束时由 DataManager 统一调用。
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    /** 获取写锁 */
    @Override
    public void lock() {
        wLock.lock();
    }

    /** 释放写锁 */
    @Override
    public void unlock() {
        wLock.unlock();
    }

    /** 获取读锁 */
    @Override
    public void rLock() {
        rLock.lock();
    }

    /** 释放读锁 */
    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    /**
     * 获取当前 DataItem 所属页面。
     *
     * @return Page 实例
     */
    @Override
    public Page page() {
        return pg;
    }

    /**
     * 获取该 DataItem 的全局唯一标识 UID。
     *
     * @return UID
     */
    @Override
    public long getUid() {
        return uid;
    }

    /**
     * 获取修改前的旧字节数组。
     *
     * @return 旧数据副本
     */
    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    /**
     * 获取包含头部与数据的完整 SubArray。
     *
     * @return 完整原始数据视图
     */
    @Override
    public SubArray getRaw() {
        return raw;
    }
}