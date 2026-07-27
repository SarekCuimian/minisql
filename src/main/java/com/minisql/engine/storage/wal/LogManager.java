package com.minisql.engine.storage.wal;

public interface LogManager extends AutoCloseable {

    LogRecord append(byte[] payload);

    void flush(long lsn);

    long getFlushedLsn();

    long getWrittenLsn();

    long getCheckpointLsn();

    void setCheckpointLsn(long lsn);

    LogReader getReader();

    @Override
    void close();

    static LogManager create(String path) {
        return LogManagerImpl.create(path);
    }

    static LogManager create(String path, int bufferSize) {
        return LogManagerImpl.create(path, bufferSize);
    }

    static LogManager open(String path) {
        return LogManagerImpl.open(path);
    }

    static LogManager open(String path, int bufferSize) {
        return LogManagerImpl.open(path, bufferSize);
    }

    interface LogReader extends AutoCloseable {
        LogRecord next();

        void rewind();

        void seek(long lsn);

        long position();

        @Override
        void close();
    }
}
