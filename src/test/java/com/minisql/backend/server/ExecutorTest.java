package com.minisql.backend.server;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import com.minisql.backend.dbm.DatabaseManager;
import org.junit.Test;

public class ExecutorTest {
    String path = "/tmp/mydb";
    long mem = (1 << 20) * 64;

    byte[] CREATE_TABLE = "create table test_table id int32 (index id)".getBytes();
    byte[] INSERT = "insert into test_table values 2333".getBytes();

    private DatabaseManager provider;

    private Executor testCreate() throws Exception {
        cleanup();
        provider = new DatabaseManager(path, mem);
        provider.createDefault();
        Executor exe = new Executor(provider);
        exe.execute("use database".getBytes());
        exe.execute(CREATE_TABLE);
        return exe;
    }

    private void testInsert(Executor exe, int times, int no) throws Exception {
        for (int i = 0; i < times; i++) {
            System.out.print(no+":"+i + ":");
            exe.execute(INSERT);
        }
    }
    
    @Test
    public void testInsert10000() throws Exception {
        Executor exe = testCreate();
        testInsert(exe, 10000, 1);
        provider.shutdown();
        cleanup();
    }

    private void testMultiInsert(int total, int noWorkers) throws Exception {
        Executor exe = testCreate();
        // 这里必须用不同的executor，否则会出现并发问题
        DatabaseManager sharedProvider = provider;
        int w = total/noWorkers;
        CountDownLatch cdl = new CountDownLatch(noWorkers);
        for(int i = 0; i < noWorkers; i ++) {
            final int no = i;
            new Thread(new Runnable(){
                @Override
                public void run() {
                    try {
                        Executor worker = new Executor(sharedProvider);
                        worker.execute("use database".getBytes());
                        testInsert(worker, w, no);
                        cdl.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        cdl.await();
    }

    @Test
    public void test100000With4() throws Exception {
        testMultiInsert(10000, 4);
        provider.shutdown();
        cleanup();
    }

    private void cleanup() {
        deleteRecursively(new File(path));
    }

    private void deleteRecursively(File file) {
        if(!file.exists()) {
            return;
        }
        if(file.isDirectory()) {
            File[] children = file.listFiles();
            if(children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
