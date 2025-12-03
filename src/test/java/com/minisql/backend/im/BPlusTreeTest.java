package com.minisql.backend.im;

import java.io.File;
import java.util.List;

import org.junit.Test;

import com.minisql.backend.dm.DataManager;
import com.minisql.backend.dm.page.cache.PageCache;
import com.minisql.backend.txm.MockTransactionManager;
import com.minisql.backend.txm.TransactionManager;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager txm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle", PageCache.PAGE_SIZE*10, txm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 10000;
        for(int i = lim-1; i >= 0; i --) {
            tree.insert(i, i);
        }

        for(int i = 0; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            assert uids.size() == 1;
            assert uids.get(0) == i;
        }

        assert new File("/tmp/TestTreeSingle.db").delete();
        assert new File("/tmp/TestTreeSingle.log").delete();
    }
}
