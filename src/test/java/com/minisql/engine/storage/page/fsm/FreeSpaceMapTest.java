package com.minisql.engine.storage.page.fsm;

import com.minisql.engine.storage.page.fsm.FreeSpaceMap;
import com.minisql.engine.storage.page.fsm.FreeSpace;
import org.junit.jupiter.api.Test;

import com.minisql.engine.storage.page.PageCache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
                assertNotNull(pi);
                assertEquals(i + 1, pi.pgno);
            }
        }
    }
}
