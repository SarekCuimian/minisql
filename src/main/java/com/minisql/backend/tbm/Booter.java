package com.minisql.backend.tbm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;

import com.minisql.backend.utils.Panic;
import com.minisql.common.Error;

// 记录第一个表的uid
public class Booter {
    public static final String BOOTER_SUFFIX = ".bt";
    public static final String BOOTER_TMP_SUFFIX = ".bt_tmp";

    String path;
    File file;

    public static Booter create(String path) {
        removeBadTmp(path);
        File f = new File(path + BOOTER_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.of(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.of(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    public static Booter open(String path) {
        removeBadTmp(path);
        File f = new File(path+BOOTER_SUFFIX);
        if(!f.exists()) {
            Panic.of(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }
        return new Booter(path, f);
    }

    private static void removeBadTmp(String path) {
        new File(path+BOOTER_TMP_SUFFIX).delete();
    }

    private Booter(String path, File file) {
        this.path = path;
        this.file = file;
    }

    public String getPath() {
        return path;
    }

    public byte[] load() {
        byte[] buf = null;
        try {
            buf = Files.readAllBytes(file.toPath());
        } catch (IOException e) {
            Panic.of(e);
        }
        return buf;
    }

    public void update(byte[] data) {
        File tmp = new File(path + BOOTER_TMP_SUFFIX);
        try {
            tmp.createNewFile();
        } catch (Exception e) {
            Panic.of(e);
        }
        if(!tmp.canRead() || !tmp.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }
        try(FileOutputStream out = new FileOutputStream(tmp)) {
            out.write(data);
            out.flush();
        } catch(IOException e) {
            Panic.of(e);
        }
        try {
            Files.move(tmp.toPath(), new File(path+BOOTER_SUFFIX).toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch(IOException e) {
            Panic.of(e);
        }
        file = new File(path+BOOTER_SUFFIX);
        if(!file.canRead() || !file.canWrite()) {
            Panic.of(Error.FileCannotRWException);
        }
    }

}
