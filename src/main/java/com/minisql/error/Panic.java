package com.minisql.error;

public class Panic {
    public static void of(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
