package com.minisql.backend.tbm;

/**
 * Holds per-thread SELECT statistics so the server layer can append
 * row-count summaries without changing the transport protocol.
 */
public class SelectStats {
    private static final ThreadLocal<Integer> ROW_COUNT = ThreadLocal.withInitial(() -> -1);

    public static void setRowCount(int count) {
        ROW_COUNT.set(count);
    }

    public static int getAndResetRowCount() {
        int count = ROW_COUNT.get();
        ROW_COUNT.set(-1);
        return count;
    }
}
