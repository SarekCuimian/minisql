package com.minisql.backend.dm.page.cache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.minisql.backend.common.AbstractCache;
import com.minisql.backend.dm.page.Page;
import com.minisql.backend.dm.page.PageImpl;
import com.minisql.backend.utils.Panic;
import com.minisql.backend.utils.FileChannelUtil;
import com.minisql.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private final RandomAccessFile file;
    private final FileChannel fc;
    private final Lock fileLock;
    private final AtomicInteger pageNumberCounter;

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int capacity) {
        super(capacity);
        if(capacity < MEM_MIN_LIM) {
            Panic.of(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.of(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumberCounter = new AtomicInteger((int)length / PAGE_SIZE);
    }

    public int newPage(byte[] initData) {
        int pgno = pageNumberCounter.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, this);
        persist(pg);
        return pgno;
    }

    public Page getPage(int pgno) throws Exception {
        return get(pgno);
    }

    public void releasePage(Page page) {
        release(page.getPageNumber());
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page
     */
    @Override
    protected Page loadCache(long key) throws Exception {
        int pgno = (int)key;
        long offset = getPageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            FileChannelUtil.readFully(fc, buf, offset);
        } catch(IOException e) {
            Panic.of(e);
        } finally {
            fileLock.unlock();
        }
        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void flushCache(Page pg) {
        if(pg.isDirty()) {
            persist(pg);
            pg.setDirty(false);
        }
    }

    /**
     * 持久化 Page
     */
    @Override
    public void persistPage(Page pg) {
        persist(pg);
    }

    /**
     * 将 Page 写入文件中
     */
    private void persist(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = getPageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            FileChannelUtil.writeFully(fc, buf, offset);
            fc.force(false);
        } catch(IOException e) {
            Panic.of(e);
        } finally {
            fileLock.unlock();
        }
    }

    public void truncateByPgno(int maxPgno) {
        // Must be called only when no page references are held; otherwise later flush can extend file.
        long size = getPageOffset(maxPgno + 1);
        fileLock.lock();
        try {
            file.setLength(size);
            pageNumberCounter.set(maxPgno);
        } catch (IOException e) {
            Panic.of(e);
        } finally {
            fileLock.unlock();
        }
    }
 
    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.of(e);
        }
    }

    public int getPageCount() {
        return pageNumberCounter.intValue();
    }

    private long getPageOffset(int pgno) {
        return (long) (pgno - 1) * PAGE_SIZE;
    }

    
}
