package com.minisql.engine.index;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minisql.engine.storage.record.RecordManager;
import com.minisql.engine.storage.page.PageCache;
import com.minisql.engine.transaction.status.MockTransactionManager;
import com.minisql.engine.transaction.status.TransactionManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BPlusTreeTest {

    @TempDir
    Path tempDir;

    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager txm = new MockTransactionManager();
        RecordManager recordManager = RecordManager.create(tempDir.resolve("TestTreeSingle").toString(), PageCache.PAGE_SIZE * 1024, txm);

        long root = BPlusTree.create(recordManager);
        BPlusTree tree = BPlusTree.load(root, recordManager);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assertEquals(1, uids.size());
            assertEquals((long) i, uids.get(0));
        }

        recordManager.close();
    }
}
