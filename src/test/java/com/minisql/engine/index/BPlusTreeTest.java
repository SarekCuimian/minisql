package com.minisql.engine.index;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.page.PageBufferPool;
import com.minisql.engine.storage.wal.ActiveTransactionTable;
import com.minisql.engine.transaction.xid.XidAllocator;
import com.minisql.engine.transaction.xid.XidStatusTable;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BPlusTreeTest {

    @TempDir
    Path tempDir;

    @Test
    public void testTreeSingle() throws Exception {
        String path = tempDir.resolve("TestTreeSingle").toString();
        XidAllocator xidAllocator = XidAllocator.create(path);
        PageRecordManager pageRecordManager = PageRecordManager.create(
                path,
                PageBufferPool.PAGE_SIZE * 1024,
                new XidStatusTable(xidAllocator),
                new ActiveTransactionTable()
        );

        long root = BPlusTree.create(pageRecordManager);
        BPlusTree tree = BPlusTree.load(root, pageRecordManager);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assertEquals(1, uids.size());
            assertEquals((long) i, uids.get(0));
        }

        pageRecordManager.close();
        xidAllocator.close();
    }
}
