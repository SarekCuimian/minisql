package com.minisql.backend.dm.pageindex;

import com.minisql.backend.dm.page.fsm.FreeSpaceMap;
import com.minisql.backend.dm.page.fsm.FreeSpace;
import org.junit.Test;

import com.minisql.backend.dm.page.cache.PageCache;

public class FreeSpaceMapTest {
    @Test
    public void testPageIndex() {
        FreeSpaceMap pIndex = new FreeSpaceMap();
        int threshold = PageCache.PAGE_SIZE / 20;
        for(int i = 0; i < 20; i ++) {
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
            pIndex.add(i, i*threshold);
        }

        for(int k = 0; k < 3; k ++) {
            for(int i = 0; i < 19; i ++) {
                FreeSpace pi = pIndex.poll(i * threshold);
                assert pi != null;
                assert pi.pgno == i+1;
            }
        }
    }
}
