package com.minisql.engine.storage.page;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Dirty Page Table：保存每个脏页首次变脏记录的 startLsn。
 */
public final class DirtyPageTable {

    private final Lock lock = new ReentrantLock();
    private final Map<Integer, Long> recoveryLsnByPage = new HashMap<>();
    private final TreeMap<Long, Integer> pageByRecoveryLsn = new TreeMap<>();

    public boolean mark(int pageNumber, long recoveryLsn) {
        if (pageNumber <= 0) {
            throw new IllegalArgumentException(
                    "pageNumber must be positive: " + pageNumber
            );
        }
        if (recoveryLsn < 0) {
            throw new IllegalArgumentException(
                    "recoveryLsn must not be negative: " + recoveryLsn
            );
        }
        lock.lock();
        try {
            if (recoveryLsnByPage.containsKey(pageNumber)) {
                return false;
            }
            recoveryLsnByPage.put(pageNumber, recoveryLsn);
            pageByRecoveryLsn.put(recoveryLsn, pageNumber);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void remove(int pageNumber, long recoveryLsn) {
        lock.lock();
        try {
            Long currentRecoveryLsn = recoveryLsnByPage.get(pageNumber);
            if (currentRecoveryLsn == null || currentRecoveryLsn != recoveryLsn) {
                return;
            }
            recoveryLsnByPage.remove(pageNumber);
            pageByRecoveryLsn.remove(currentRecoveryLsn);
        } finally {
            lock.unlock();
        }
    }

    public Long getRecoveryLsn(int pageNumber) {
        lock.lock();
        try {
            return recoveryLsnByPage.get(pageNumber);
        } finally {
            lock.unlock();
        }
    }

    public long getMinRecoveryLsn() {
        lock.lock();
        try {
            return pageByRecoveryLsn.isEmpty()
                    ? Long.MAX_VALUE
                    : pageByRecoveryLsn.firstKey();
        } finally {
            lock.unlock();
        }
    }

    public Map<Integer, Long> snapshot() {
        lock.lock();
        try {
            return Map.copyOf(recoveryLsnByPage);
        } finally {
            lock.unlock();
        }
    }

    public List<DirtyPage> getBatch(int maximumSize) {
        if (maximumSize <= 0) {
            return Collections.emptyList();
        }
        lock.lock();
        try {
            List<DirtyPage> pages = new ArrayList<>(
                    Math.min(maximumSize, pageByRecoveryLsn.size())
            );
            for (Map.Entry<Long, Integer> entry : pageByRecoveryLsn.entrySet()) {
                pages.add(new DirtyPage(entry.getValue(), entry.getKey()));
                if (pages.size() == maximumSize) {
                    break;
                }
            }
            return pages;
        } finally {
            lock.unlock();
        }
    }

    public boolean isEmpty() {
        lock.lock();
        try {
            return recoveryLsnByPage.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    public static final class DirtyPage {
        private final int pageNumber;
        private final long recoveryLsn;

        private DirtyPage(int pageNumber, long recoveryLsn) {
            this.pageNumber = pageNumber;
            this.recoveryLsn = recoveryLsn;
        }

        public int getPageNumber() {
            return pageNumber;
        }

        public long getRecoveryLsn() {
            return recoveryLsn;
        }
    }
}
