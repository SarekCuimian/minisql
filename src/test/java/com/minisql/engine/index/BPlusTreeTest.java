package com.minisql.engine.index;

import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.minisql.engine.storage.StorageTestContext;
import com.minisql.engine.storage.record.PageRecordManager;
import com.minisql.engine.storage.page.PageBufferPool;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BPlusTreeTest {

    @TempDir
    Path tempDir;

    @Test
    public void testTreeSingle() throws Exception {
        String path = tempDir.resolve("TestTreeSingle").toString();
        try (StorageTestContext storage = StorageTestContext.create(
                path,
                PageBufferPool.PAGE_SIZE * 1024L
        )) {
            PageRecordManager pageRecordManager =
                    storage.getPageRecordManager();

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
        }
    }
}
